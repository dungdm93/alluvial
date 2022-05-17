package dev.alluvial.source.kafka

import dev.alluvial.api.Inlet
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.ArrayDeque
import java.util.PriorityQueue
import java.util.Queue

@Suppress("MemberVisibilityCanBePrivate")
class KafkaTopicInlet(
    val topic: String,
    private val consumer: Consumer<ByteArray, ByteArray>,
    private val converter: KafkaConverter,
    private val pollTimeout: Duration,
) : Inlet {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaTopicInlet::class.java)
    }

    @Suppress("JoinDeclarationAndAssignment")
    private val partitions: Set<TopicPartition>
    private val partitionQueues = mutableMapOf<Int, Queue<SinkRecord>>()
    private val heap = PriorityQueue(Comparator.comparing(SinkRecord::timestamp))
    private var metrics: KafkaClientMetrics? = null

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
        var consumerRecords = consumer.poll(pollTimeout)
        while (consumer.assignment().isEmpty() && consumerRecords.isEmpty) {
            logger.info("Waiting for partitions assigned to the consumer")
            consumerRecords = consumer.poll(pollTimeout)
        }

        consumerRecords.partitions().forEach { tp ->
            val queue = partitionQueues.computeIfAbsent(tp.partition(), ::ArrayDeque)
            val previousEmpty = queue.isEmpty()
            consumerRecords.records(tp).forEach {
                val record = converter.convert(it)
                queue.add(record)
            }
            val record = queue.peek()
            if (previousEmpty && record != null) {
                heap.add(record)
            }
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
                val com = committedOffsets.map { (tp, oam) -> "${tp.partition()}:${oam.offset()}" }
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

    fun enableMetrics(registry: MeterRegistry, tags: Iterable<Tag>) {
        metrics = KafkaClientMetrics(consumer, tags).also { it.bindTo(registry) }
    }

    override fun close() {
        consumer.close()
        partitionQueues.clear()
        heap.clear()
        metrics?.close()
    }

    override fun toString(): String {
        return "KafkaTopicInlet($topic)"
    }
}
