package dev.alluvial.schema.debezium

import dev.alluvial.api.SchemaHandler
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.fieldSchema
import org.apache.iceberg.UpdateProperties
import org.apache.iceberg.UpdateSchema
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Handle schema changes based on schema of Kafka message's value
 */
class KafkaSchemaSchemaHandler(
    outlet: IcebergTableOutlet,
) : SchemaHandler {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(KafkaSchemaSchemaHandler::class.java)
    }

    private val table = outlet.table
    var schemaVersion: String? = null
        private set

    init {
        // Try to fetch schema version from table properties
        schemaVersion = table.properties()[SCHEMA_VERSION_PROP]
        if (schemaVersion == null) {
            logger.warn("Could not get schema version from table properties")
        }
    }

    override fun shouldMigrate(record: SinkRecord): Boolean {
        if (record.value() == null) return false

        // Current schema version is null, so should perform migration to ensure the version is updated
        // and new schema is applied (if any)
        if (schemaVersion == null) return true

        val version = record.schemaVersion()
        return schemaVersion != version
    }

    override fun migrateSchema(record: SinkRecord) {
        logger.info("Start to migrate table schema")

        val txn = table.newTransaction()
        updateSchema(txn.updateSchema(), record)

        val version = record.schemaVersion()
        updateSchemaVersion(txn.updateProperties(), version)
        txn.commitTransaction()

        schemaVersion = version
    }

    private fun updateSchema(updater: UpdateSchema, record: SinkRecord) {
        val keySchema = record.keySchema()
        val valueSchema = record.valueSchema()

        val after = valueSchema.fieldSchema("after")
        val migrator = KafkaSchemaSchemaMigrator(updater)
        migrator.visit(after, table.schema())

        val keys = keySchema.fields().map { it.name() }.toSet()
        if (keys != table.schema().identifierFieldNames())
            updater.setIdentifierFields(keys)
        updater.commit()
    }

    private fun updateSchemaVersion(updater: UpdateProperties, version: String) {
        updater.set(SCHEMA_VERSION_PROP, version)
        updater.commit()
    }
}
