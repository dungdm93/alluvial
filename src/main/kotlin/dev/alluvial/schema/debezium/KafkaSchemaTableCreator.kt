package dev.alluvial.schema.debezium

import dev.alluvial.api.StreamletId
import dev.alluvial.api.TableCreator
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.data.toIcebergSchema
import dev.alluvial.source.kafka.KafkaSource
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema.Type.STRUCT
import org.apache.kafka.connect.sink.SinkRecord
import java.time.Duration
import org.apache.iceberg.Schema as IcebergSchema

class KafkaSchemaTableCreator(
    private val source: KafkaSource,
    private val sink: IcebergSink,
) : TableCreator {
    private val locationBase: String? = null

    override fun createTable(id: StreamletId): Table {
        val consumer = source.newConsumer<ByteArray, ByteArray>()
        val converter = source.getConverter()

        setupConsumer(consumer, id)
        return consumer.firstNonNull {
            val record = converter.convert(it)
            buildTable(id, record)
        }
    }

    private fun <K, V> setupConsumer(consumer: KafkaConsumer<K, V>, id: StreamletId) {
        val topic = source.topicOf(id)

        val topicPartitions = consumer.partitionsFor(topic)
            .map { TopicPartition(it.topic(), it.partition()) }
        consumer.assign(topicPartitions)
        consumer.seekToBeginning(topicPartitions)
    }

    private inline fun <K, V, R> KafkaConsumer<K, V>.firstNonNull(block: (ConsumerRecord<K, V>) -> R): R {
        val r: ConsumerRecord<K, V>
        loop@ while (true) {
            val records = this.poll(Duration.ofSeconds(1))
            if (records == null || records.isEmpty) continue

            for (record in records) {
                if (record.value() != null) {
                    r = record
                    break@loop
                }
            }
        }
        return block(r)
    }

    private fun buildTable(id: StreamletId, record: SinkRecord): Table {
        val iSchema = icebergSchemaFrom(record)
        val tableBuilder = sink.newTableBuilder(id, iSchema)
            .withProperty(TableProperties.FORMAT_VERSION, "2")

        // TODO other table attributes
        if (locationBase != null)
            tableBuilder.withLocation("$locationBase/${id.schema}/${id.table}")

        return tableBuilder.create()
    }

    private fun icebergSchemaFrom(record: SinkRecord): IcebergSchema {
        val keySchema = record.keySchema()
        val valueSchema = record.valueSchema()
        require(keySchema.type() == STRUCT) { "KeySchema must be a STRUCT" }
        require(valueSchema.type() == STRUCT) { "ValueSchema must be a STRUCT" }

        val rowSchema = valueSchema.field("after").schema()
        require(rowSchema.type() == STRUCT) { "ValueSchema.after must be a STRUCT" }

        val keys = keySchema.fields().map { it.name() }
        return rowSchema.toIcebergSchema(keys)
    }
}
