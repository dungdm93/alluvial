package dev.alluvial.source.kafka

import dev.alluvial.api.Inlet
import dev.alluvial.api.StreamletId
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.ArrayDeque
import java.util.PriorityQueue
import java.util.Queue

class KafkaTopicInlet(
    val id: StreamletId,
    val topic: String,
    config: Map<String, Any>,
) : Inlet {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaTopicInlet::class.java)
    }

    private val consumer: Consumer<ByteArray, ByteArray>
    private val converter: KafkaConverter
    private val pendingSeekingOffsets = mutableMapOf<Int, Long>()
    private val partitionQueues = mutableMapOf<Int, Queue<SinkRecord>>()
    private val heap = PriorityQueue(Comparator.comparing(SinkRecord::timestamp))
    private val requestTimeout = Duration.ofSeconds(1)

    init {
        consumer = KafkaConsumer(config)
        converter = KafkaConverter(config)

        val listener = object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {}

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                partitions.forEach { tp ->
                    // alternative: using admin.alterConsumerGroupOffsets but it require admin privilege
                    val offset = pendingSeekingOffsets.remove(tp.partition())
                    if (offset != null) consumer.seek(tp, offset)
                }
            }
        }
        consumer.subscribe(listOf(topic), listener)
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
        var consumerRecords = consumer.poll(requestTimeout)
        while (consumer.assignment().isEmpty() && consumerRecords.isEmpty) {
            logger.info("Waiting for partitions assigned to the consumer")
            consumerRecords = consumer.poll(requestTimeout)
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
        val partitions = consumer.assignment()
        consumer.pause(partitions)
    }

    fun resume() {
        val partitions = consumer.assignment()
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
        val tp = partitions()
        return consumer.committed(tp)
            .map { (tp, oam) -> tp.partition() to oam?.offset() }
            .toMap()
    }

    fun seekOffsets(offsets: Map<Int, Long>) {
        pendingSeekingOffsets.putAll(offsets)
    }

    fun currentLag(): Long {
        val partitions = partitions()
        val endOffsets = consumer.endOffsets(partitions)
        val committedOffsets = committedOffsets()

        var lag = 0L
        endOffsets.forEach { (tp, endOffset) ->
            val currentOffset = committedOffsets[tp.partition()] ?: 0L
            lag += endOffset - currentOffset
        }
        return lag
    }

    private fun partitions(): Set<TopicPartition> {
        val partitions = consumer.partitionsFor(topic)
        return partitions
            .map { TopicPartition(it.topic(), it.partition()) }
            .toSet()
    }

    override fun close() {
        consumer.close()
    }

    override fun toString(): String {
        return "KafkaTopicInlet($topic)"
    }
}
