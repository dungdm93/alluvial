package dev.alluvial.api

import org.apache.kafka.connect.sink.SinkRecord

interface SchemaHandler {
    val id: StreamletId
    fun shouldMigrate(record: SinkRecord): Boolean
    fun migrateSchema(record: SinkRecord)
}
