package dev.alluvial.schema.debezium

import dev.alluvial.api.StreamletId
import dev.alluvial.api.TableCreator
import dev.alluvial.runtime.PartitionSpecConfig
import dev.alluvial.runtime.TableCreationConfig
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import dev.alluvial.source.kafka.KafkaSource
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.PartitionSpecs
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
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
    tableCreationConfig: TableCreationConfig,
) : TableCreator {
    companion object {
        const val TABLE_FORMAT_VERSION = "2"
    }

    private val properties = tableCreationConfig.properties
    private val partitionSpec = tableCreationConfig.partitionSpec
    private val baseLocation = tableCreationConfig.baseLocation?.trimEnd('/')

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
        val tableId = sink.tableIdentifierOf(id)
        val tableBuilder = sink.newTableBuilder(tableId, iSchema)
            .withProperty(TableProperties.FORMAT_VERSION, TABLE_FORMAT_VERSION)

        tableBuilder.withProperties(properties)
        if (baseLocation != null)
            tableBuilder.withLocation("${baseLocation}/${id.schema}/${id.table}")

        val partitionConfigs = partitionSpec[id.table]
        if (!partitionConfigs.isNullOrEmpty()) {
            val partitionSpec = buildPartitionSpec(iSchema, partitionConfigs)
            tableBuilder.withPartitionSpec(partitionSpec)
        }

        sink.ensureNamespace(tableId.namespace())
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

    private fun buildPartitionSpec(schema: IcebergSchema, partitionConfigs: List<PartitionSpecConfig>): PartitionSpec {
        val builder = PartitionSpec.builderFor(schema)
        partitionConfigs.forEach { config ->
            val sourceName = config.column
            val sourceField = schema.findField(sourceName)
            Preconditions.checkNotNull(sourceField, "Cannot find source field: %s", sourceName)
            PartitionSpecs.add(builder, sourceField, config.transform)
        }
        return builder.build()
    }
}
