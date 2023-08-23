package dev.alluvial.source.kafka

import dev.alluvial.api.Inlet
import dev.alluvial.utils.withSpan
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
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
    val name: String,
    val topic: String,
    private val consumer: Consumer<ByteArray?, ByteArray?>,
    private val converter: KafkaConverter,
    private val pollTimeout: Duration,
    private val tracer: Tracer,
    private val meter: Meter,
) : Inlet {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaTopicInlet::class.java)
        private val kafkaTimestampComparator = Comparator.comparing(SinkRecord::timestamp)
        private val debeziumTimestampComparator = Comparator<SinkRecord> { o1, o2 ->
            val o1Ts = o1.debeziumTimestamp()!!
            val o2T2 = o2.debeziumTimestamp()!!
            o1Ts.compareTo(o2T2)
        }
        private val sourceTimestampComparator = Comparator<SinkRecord> { o1, o2 ->
            val o1Ts = o1.sourceTimestamp()!!
            val o2T2 = o2.sourceTimestamp()!!
            o1Ts.compareTo(o2T2)
        }

        private val comparator = Comparator<SinkRecord> { o1, o2 ->
            if (o1.value() != null && o2.value() != null) {
                val res = sourceTimestampComparator.compare(o1, o2)
                if (res != 0) return@Comparator res
            }
            kafkaTimestampComparator.compare(o1, o2)
        }
    }

    @Suppress("JoinDeclarationAndAssignment")
    private val partitions: Set<TopicPartition>
    private val partitionQueues = mutableMapOf<Int, Queue<SinkRecord>>()
    private val heap = PriorityQueue(comparator)

    private val attrs = Attributes.of(stringKey("alluvial.inlet"), name)

    init {
        partitions = consumer.partitionsFor(topic).map {
            TopicPartition(it.topic(), it.partition())
        }.toSet()

        consumer.assign(partitions)

        meter.gaugeBuilder("alluvial.inlet.partition.queue.size").ofLongs()
            .buildWithCallback {
                val totalSize = partitionQueues.values.sumOf(Collection<*>::size).toLong()
                it.record(totalSize, attrs)
            }
    }

    /**
     * Read a record from kafka in timestamp order and ignore Tombstone events
     */
    fun read(): SinkRecord? = tracer.withSpan("KafkaTopicInlet.read", attrs) {
        val record = pollFromQueues()
        if (record == null) {
            pollFromBrokers()
            return pollFromQueues()
        }

        val queue = partitionQueues[record.kafkaPartition()]!!
        if (queue.isEmpty()) pollFromBrokers()
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

    private fun pollFromBrokers() {
        logger.info("Polling data from Kafka brokers")
        // first time, poll from all partitions
        consumer.resume(partitions)
        val pausedPartitions = mutableSetOf<TopicPartition>()

        do {
            consumer.pause(pausedPartitions)

            val consumerRecords = consumer.poll(pollTimeout)
            if (logger.isInfoEnabled) {
                logger.info(
                    "Polled {} records in partitions {}",
                    consumerRecords.count(),
                    consumerRecords.partitions().map(TopicPartition::partition)
                )
            }
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

    private fun queueRecords(tp: TopicPartition, records: List<ConsumerRecord<ByteArray?, ByteArray?>>) {
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

    fun commit(positions: Map<Int, Long>) = tracer.withSpan("KafkaTopicInlet.commit", attrs) {
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

    fun seekOffsets(offsets: Map<Int, Long>) = tracer.withSpan("KafkaTopicInlet.seekOffsets", attrs) {
        offsets.forEach { (partition, offset) ->
            consumer.seek(TopicPartition(topic, partition), offset)
        }
        commit(offsets)
    }

    fun currentLag(): Long = tracer.withSpan("KafkaTopicInlet.currentLag", attrs) {
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
    }

    override fun toString(): String {
        return "KafkaTopicInlet($topic)"
    }
}
