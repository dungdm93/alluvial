package dev.alluvial.source.kafka

import dev.alluvial.api.Inlet
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Duration
import java.util.ArrayDeque
import java.util.PriorityQueue
import java.util.Queue

@Suppress("MemberVisibilityCanBePrivate")
class KafkaTopicInlet(
    val name: String,
    val topic: String,
    private val consumer: Consumer<ByteArray, ByteArray>,
    private val converter: KafkaConverter,
    private val pollTimeout: Duration,
    registry: MeterRegistry,
) : Inlet {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaTopicInlet::class.java)
    }

    @Suppress("JoinDeclarationAndAssignment")
    private val partitions: Set<TopicPartition>
    private val partitionQueues = mutableMapOf<Int, Queue<SinkRecord>>()
    private val heap = PriorityQueue(Comparator.comparing(SinkRecord::timestamp))
    private val metrics = Metrics(registry)

    init {
        partitions = consumer.partitionsFor(topic).map {
            TopicPartition(it.topic(), it.partition())
        }.toSet()

        consumer.assign(partitions)
    }

    /**
     * Read a record from kafka in timestamp order and ignore Tombstone events
     */
    fun read(): SinkRecord? {
        val record = pollFromQueues()
        if (record == null) {
            pullFromBrokers()
            return pollFromQueues()
        }

        val queue = partitionQueues[record.kafkaPartition()]!!
        if (queue.isEmpty()) pullFromBrokers()
        return record
    }

    private fun pollFromQueues(): SinkRecord? {
        val record = heap.poll() ?: return null
        val queue = partitionQueues[record.kafkaPartition()]!!
        queue.remove()
        val nextRecord = queue.peek()
        if (nextRecord != null) heap.add(nextRecord)
        return record
    }

    private fun pullFromBrokers() {
        // first time, poll from all partitions
        consumer.resume(partitions)
        val pausedPartitions = mutableSetOf<TopicPartition>()

        do {
            consumer.pause(pausedPartitions)

            val consumerRecords = consumer.poll(pollTimeout)
            logger.info(
                "Polled {} records in {} partitions",
                consumerRecords.count(), consumerRecords.partitions().size
            )
            if (consumerRecords.isEmpty) return // no more data

            consumerRecords.partitions().forEach { tp ->
                val records = consumerRecords.records(tp)
                queueRecords(tp, records)
            }

            // consumer.poll(...) doesn't return data of all partitions
            // So, next times, only poll from partitions which its queue is empty
            partitions.filterTo(pausedPartitions) {
                !partitionQueues[it.partition()].isNullOrEmpty()
            }
        } while (pausedPartitions.size < partitions.size)
    }

    private fun queueRecords(tp: TopicPartition, records: List<ConsumerRecord<ByteArray, ByteArray>>) {
        if (records.isEmpty()) return

        // add records to queue
        val queue = partitionQueues.computeIfAbsent(tp.partition(), ::ArrayDeque)
        val previousEmpty = queue.isEmpty()
        records.forEach {
            val record = converter.convert(it)
            queue.add(record)
        }

        // update heap
        val headRecord = queue.peek()
        if (previousEmpty && headRecord != null) {
            heap.add(headRecord)
        }
    }

    fun pause() {
        consumer.pause(partitions)
    }

    fun resume() {
        consumer.resume(partitions)
    }

    fun commit(positions: Map<Int, Long>) {
        val offsets = buildMap(positions.size) {
            positions.forEach { (partition, offset) ->
                val tp = TopicPartition(topic, partition)
                val oam = OffsetAndMetadata(offset)
                put(tp, oam)
            }
        }
        consumer.commitAsync(offsets) { committedOffsets, exception ->
            if (exception != null) {
                logger.error("Error while commit offset", exception)
            } else {
                val com = buildMap(committedOffsets.size) {
                    committedOffsets.forEach { (tp, oam) -> put(tp.partition(), oam.offset()) }
                }
                logger.info("Committed offsets {}", com)
            }
        }
    }

    fun committedOffsets(): Map<Int, Long?> {
        return consumer.committed(partitions)
            .map { (tp, oam) -> tp.partition() to oam?.offset() }
            .toMap()
    }

    fun seekOffsets(offsets: Map<Int, Long>) {
        offsets.forEach { (partition, offset) ->
            consumer.seek(TopicPartition(topic, partition), offset)
        }
        commit(offsets)
    }

    fun currentLag(): Long {
        val endOffsets = consumer.endOffsets(partitions)
        val committedOffsets = committedOffsets()

        var lag = 0L
        endOffsets.forEach { (tp, endOffset) ->
            val currentOffset = committedOffsets[tp.partition()] ?: 0L
            lag += endOffset - currentOffset
        }

        return lag
    }

    override fun close() {
        consumer.close()
        partitionQueues.clear()
        heap.clear()
        metrics.close()
    }

    override fun toString(): String {
        return "KafkaTopicInlet($topic)"
    }

    private inner class Metrics(private val registry: MeterRegistry) : Closeable {
        private val tags = Tags.of("inlet", name)

        private val kafkaClientMetrics = KafkaClientMetrics(consumer, tags)
            .also { it.bindTo(registry) }

        private val queueSize = Gauge.builder(
            "alluvial.inlet.partition.queue.size", this@KafkaTopicInlet.partitionQueues
        ) { it.values.sumOf(Collection<*>::size).toDouble() }
            .tags(tags)
            .description("Total size of all inlet partition queues")
            .register(registry)

        override fun close() {
            registry.remove(queueSize)
            queueSize.close()
            kafkaClientMetrics.close()
        }
    }
}
