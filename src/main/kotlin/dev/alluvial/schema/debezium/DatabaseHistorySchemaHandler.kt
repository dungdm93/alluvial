package dev.alluvial.schema.debezium

import dev.alluvial.api.SchemaHandler
import org.apache.kafka.connect.sink.SinkRecord

/**
 * Handle schema changes based on DatabaseHistory topic
 */
class DatabaseHistorySchemaHandler : SchemaHandler {
    override fun shouldMigrate(record: SinkRecord): Boolean {
        TODO("Not yet implemented")
    }

    override fun migrateSchema(record: SinkRecord) {
        TODO("Not yet implemented")
    }
}
