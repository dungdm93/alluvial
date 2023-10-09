package dev.alluvial.schema.debezium

import dev.alluvial.api.TableCreator
import dev.alluvial.runtime.PartitionSpecConfig
import dev.alluvial.runtime.TableCreationConfig
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import dev.alluvial.source.kafka.KafkaSource
import dev.alluvial.source.kafka.fieldSchema
import dev.alluvial.utils.withSpan
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.SortOrder
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.transforms.Transforms
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema.Type.STRUCT
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import java.time.Duration

class KafkaSchemaTableCreator(
    tableCreationConfig: TableCreationConfig,
    private val source: KafkaSource,
    private val sink: IcebergSink,
    private val tracer: Tracer,
    private val meter: Meter,
) : TableCreator {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaSchemaTableCreator::class.java)
        const val TABLE_FORMAT_VERSION = "2"
        private val pollTimeout: Duration = Duration.ofSeconds(1)
    }

    private val properties = tableCreationConfig.properties
    private val partitionSpec = tableCreationConfig.partitionSpec
    private val baseLocation = tableCreationConfig.baseLocation?.trimEnd('/')

    override fun createTable(topic: String, tableId: TableIdentifier): Table = tracer.withSpan(
        "KafkaSchemaTableCreator.createTable"
    ) {
        val record = findSchemaRecord(topic, tableId)
        val table = buildTable(tableId, record)
        val partitionConfigs = partitionSpec[tableId.name()]
        if (!partitionConfigs.isNullOrEmpty()) {
            updatePartitionSpec(table, partitionConfigs)
        }

        return table
    }

    private fun findSchemaRecord(topic: String, tableId: TableIdentifier): SinkRecord = tracer.withSpan(
        "KafkaSchemaTableCreator.findSchemaRecord"
    ) {
        val overrideConfig = mapOf(ConsumerConfig.CLIENT_ID_CONFIG to "${tableId}(creator)")
        val consumer = source.newConsumer<ByteArray?, ByteArray?>(overrideConfig)
        val converter = source.getConverter()

        consumer.use {
            setupConsumer(consumer, topic)
            consumer.firstNonNull(converter::convert)
        }
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

    private fun buildTable(tableId: TableIdentifier, record: SinkRecord): Table = tracer.withSpan(
        "KafkaSchemaTableCreator.buildTable"
    ) {
        val iSchema = icebergSchemaFrom(record)
        val schemaVersion = record.schemaVersion()

        val table = sink.buildTable(tableId, iSchema) { builder ->
            logger.info("Creating table {}", tableId)
            logger.info("schema: version: {}\n{}", schemaVersion, iSchema)

            builder.withProperty(TableProperties.FORMAT_VERSION, TABLE_FORMAT_VERSION)
            logger.info("formatVersion: {}", TABLE_FORMAT_VERSION)

            builder.withProperty(SCHEMA_VERSION_PROP, schemaVersion)

            builder.withProperties(properties)
            logger.info("properties: {}", properties)

            if (baseLocation != null) {
                val nsPath = tableId.namespace().levels().joinToString("/")
                val location = "${baseLocation}/${nsPath}/${tableId.name()}"
                builder.withLocation(location)
                logger.info("location: {}", location)
            }

            // TableMetadata MUST have SortOrder.unsorted() and PartitionSpec.unpartitioned()
            // with correct ID for later used.
            builder.withSortOrder(SortOrder.unsorted())
            builder.withPartitionSpec(PartitionSpec.unpartitioned())
        }
        return table
    }

    private fun updatePartitionSpec(table: Table, partitionConfigs: List<PartitionSpecConfig>) = tracer.withSpan(
        "KafkaSchemaTableCreator.updatePartitionSpec"
    ) {
        val schema = table.schema()
        val updateSpec = table.updateSpec()

        partitionConfigs.forEach {
            if ("identity".equals(it.transform, true)) {
                val ref = Expressions.ref<Any>(it.column)
                updateSpec.addField(it.name, ref)
            } else {
                val field = schema.findField(it.column)
                val transform = Transforms.fromString(field.type(), it.transform)
                val term = Expressions.transform(field.name(), transform)
                updateSpec.addField(it.name, term)
            }
        }

        updateSpec.commit()
        logger.info("partitionSpec: {}", table.spec())
    }

    private fun icebergSchemaFrom(record: SinkRecord): IcebergSchema {
        val keySchema = record.keySchema()
        val valueSchema = record.valueSchema()
        require(keySchema.type() == STRUCT) { "KeySchema must be a STRUCT" }
        require(valueSchema.type() == STRUCT) { "ValueSchema must be a STRUCT" }

        val rowSchema = valueSchema.fieldSchema("after")
        require(rowSchema.type() == STRUCT) { "ValueSchema.after must be a STRUCT" }

        val keys = keySchema.fields().map { it.name() }
        return rowSchema.toIcebergSchema(keys)
    }
}
