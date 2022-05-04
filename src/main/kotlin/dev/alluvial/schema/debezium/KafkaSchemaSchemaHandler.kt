package dev.alluvial.schema.debezium

import dev.alluvial.api.SchemaHandler
import dev.alluvial.api.StreamletId
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import org.apache.kafka.connect.sink.SinkRecord
import java.util.Objects

/**
 * Handle schema changes based on schema of Kafka message's value
 */
class KafkaSchemaSchemaHandler(
    override val id: StreamletId,
    outlet: IcebergTableOutlet,
) : SchemaHandler {
    private val table = outlet.table
    private var hashedSchema: Int? = null

    override fun shouldMigrate(record: SinkRecord): Boolean {
        if (record.value() == null) return false

        val keySchema = record.keySchema()
        val valueSchema = record.valueSchema()
        val hashed = Objects.hash(keySchema, valueSchema)

        return hashedSchema == hashed
    }

    override fun migrateSchema(record: SinkRecord) {
        val schemaUpdater = table.updateSchema()
        val keySchema = record.keySchema()
        val valueSchema = record.valueSchema()

        val migrator = KafkaSchemaSchemaMigrator(schemaUpdater)
        migrator.visit(valueSchema, table.schema())

        val keys = keySchema.fields().map { it.name() }.toSet()
        if (keys != table.schema().identifierFieldNames())
            schemaUpdater.setIdentifierFields(keys)

        schemaUpdater.commit()
        hashedSchema = Objects.hash(keySchema, valueSchema)
    }
}
