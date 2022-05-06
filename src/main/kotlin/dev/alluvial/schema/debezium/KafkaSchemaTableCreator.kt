package dev.alluvial.schema.debezium

import dev.alluvial.api.TableCreator
import dev.alluvial.runtime.PartitionSpecConfig
import dev.alluvial.runtime.TableCreationConfig
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import dev.alluvial.source.kafka.KafkaSource
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.PartitionSpecs
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema.Type.STRUCT
import org.apache.kafka.connect.sink.SinkRecord
import java.time.Duration

class KafkaSchemaTableCreator(
    private val source: KafkaSource,
    private val sink: IcebergSink,
    tableCreationConfig: TableCreationConfig,
) : TableCreator {
    companion object {
        const val TABLE_FORMAT_VERSION = "2"
        private val pollTimeout: Duration = Duration.ofSeconds(1)
    }

    private val properties = tableCreationConfig.properties
    private val partitionSpec = tableCreationConfig.partitionSpec
    private val baseLocation = tableCreationConfig.baseLocation?.trimEnd('/')

    override fun createTable(topic: String, tableId: TableIdentifier): Table {
        val consumer = source.newConsumer<ByteArray, ByteArray>()
        val converter = source.getConverter()

        setupConsumer(consumer, topic)
        val record = consumer.firstNonNull(converter::convert)
        consumer.close()
        return buildTable(tableId, record)
    }

    private fun <K, V> setupConsumer(consumer: KafkaConsumer<K, V>, topic: String) {
        val topicPartitions = consumer.partitionsFor(topic)
            .map { TopicPartition(it.topic(), it.partition()) }
        consumer.assign(topicPartitions)
        consumer.seekToBeginning(topicPartitions)
    }

    private inline fun <K, V, R> KafkaConsumer<K, V>.firstNonNull(block: (ConsumerRecord<K, V>) -> R): R {
        val r: ConsumerRecord<K, V>
        loop@ while (true) {
            val records = this.poll(pollTimeout)
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

    private fun buildTable(tableId: TableIdentifier, record: SinkRecord): Table {
        val iSchema = icebergSchemaFrom(record)

        return sink.buildTable(tableId, iSchema) { builder ->
            builder.withProperty(TableProperties.FORMAT_VERSION, TABLE_FORMAT_VERSION)

            builder.withProperties(properties)
            if (baseLocation != null) {
                val nsPath = tableId.namespace().levels().joinToString("/")
                builder.withLocation("${baseLocation}/${nsPath}/${tableId.name()}")
            }

            val partitionConfigs = partitionSpec[tableId.name()]
            if (!partitionConfigs.isNullOrEmpty()) {
                val partitionSpec = buildPartitionSpec(iSchema, partitionConfigs)
                builder.withPartitionSpec(partitionSpec)
            }
        }
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
