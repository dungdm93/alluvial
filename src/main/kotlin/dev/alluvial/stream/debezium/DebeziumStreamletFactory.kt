package dev.alluvial.stream.debezium

import dev.alluvial.api.StreamletId
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.sink.iceberg.data.KafkaSchemaUtil
import dev.alluvial.source.kafka.KafkaSource
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import java.time.Duration

class DebeziumStreamletFactory(
    private val source: KafkaSource,
    private val sink: IcebergSink,
) {
    private val converter = source.createConverter()
    private val consumer = source.createConsumer()

    fun createStreamlet(id: StreamletId): DebeziumStreamlet {
        val inlet = source.getInlet(id)
        val outlet = sink.getOutlet(id) ?: createOutlet(id)

        return DebeziumStreamlet(id, inlet, outlet)
    }

    private fun createOutlet(id: StreamletId): IcebergTableOutlet {
        val topic = source.topicOf(id)
        val topicPartitions = consumer.partitionsFor(topic)
            .map { TopicPartition(it.topic(), it.partition()) }
        consumer.assign(topicPartitions)
        consumer.seekToBeginning(topicPartitions)

        val record = retrieveFirstNonNullRecord(consumer)
        return createTableFromRecord(id, record)
    }

    private fun retrieveFirstNonNullRecord(consumer: KafkaConsumer<ByteArray, ByteArray>): SinkRecord {
        while (true) {
            val consumerRecords = consumer.poll(Duration.ofSeconds(1))
            if (consumerRecords == null || consumerRecords.isEmpty) continue

            for (record in consumerRecords) {
                if (record.value() != null)
                    return converter.convert(record)
            }
        }
    }

    private fun createTableFromRecord(id: StreamletId, record: SinkRecord): IcebergTableOutlet {
        val keySchema = record.keySchema()
        val valueSchema = record.valueSchema()
        require(keySchema.type() == Schema.Type.STRUCT) { "KeySchema must be a STRUCT" }
        require(valueSchema.type() == Schema.Type.STRUCT) { "ValueSchema must be a STRUCT" }

        val rowSchema = valueSchema.field("after").schema()
        require(rowSchema.type() == Schema.Type.STRUCT) { "ValueSchema.after must be a STRUCT" }

        val keys = keySchema.fields().map { it.name() }
        val iSchema = KafkaSchemaUtil.toIcebergSchema(rowSchema, keys)

        return sink.createOutlet(id, iSchema)
    }
}
