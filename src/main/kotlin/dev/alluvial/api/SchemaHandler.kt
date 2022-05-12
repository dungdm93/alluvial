package dev.alluvial.api

import org.apache.kafka.connect.sink.SinkRecord

interface SchemaHandler {
    fun shouldMigrate(record: SinkRecord): Boolean
    fun migrateSchema(record: SinkRecord)
}
