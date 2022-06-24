package dev.alluvial.schema.debezium

import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.structSchema
import org.apache.kafka.connect.data.Schema.INT32_SCHEMA
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class TestHelpers {
    private fun createRecord(keySchemaVersion: Int, valueSchemaVersion: Int): SinkRecord {
        val keySchema = structSchema(keySchemaVersion) { field("field1", INT32_SCHEMA) }
        val keyStruct = KafkaStruct(keySchema).put("field1", 100)
        val valueSchema = structSchema(valueSchemaVersion) { field("field1", INT32_SCHEMA) }
        val valueStruct = KafkaStruct(valueSchema).put("field1", 200)
        return SinkRecord("topic", 0, keySchema, keyStruct, valueSchema, valueStruct, 0)
    }

    @Test
    fun recordVersionBasedOnSchemaVersion() {
        val record = createRecord(1, 2)
        val expectedVersion = "key=1,value=2"
        val actualVersion = record.schemaVersion()

        Assertions.assertEquals(expectedVersion, actualVersion)
    }

    @Test
    fun recordVersionBasedOnKeySchemaVersion() {
        val valueSchemaVersion = 2
        val record = createRecord(1, valueSchemaVersion)
        val expectedVersion = "key=2,value=2"
        val actualVersion = record.schemaVersion()

        Assertions.assertNotEquals(expectedVersion, actualVersion)
    }

    @Test
    fun recordVersionBasedOnValueSchemaVersion() {
        val keySchemaVersion = 2
        val record = createRecord(keySchemaVersion, 2)
        val expectedVersion = "key=2,value=1"
        val actualVersion = record.schemaVersion()

        Assertions.assertNotEquals(expectedVersion, actualVersion)
    }
}
