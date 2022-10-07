package dev.alluvial.dedupe.rocksdb

import dev.alluvial.dedupe.backend.rocksdb.AvroSerializer
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.structSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestAvroSerializer {
    private val serializer = AvroSerializer(emptyMap())

    private fun createRecord(id: Int, type: String): SinkRecord {
        val schema = structSchema {
            field("id", Schema.INT32_SCHEMA)
            field("type", Schema.STRING_SCHEMA)
        }
        val value = KafkaStruct(schema)
            .put("id", id)
            .put("type", type)
        return SinkRecord("topic", 1, schema, value, schema, value, 0)
    }

    @Test
    fun testSerialize() {
        val record = createRecord(1, "typeA")
        val keyBytes = serializer.serialize(record)

        val sameRecord = createRecord(1, "typeA")
        val sameKeyBytes = serializer.serialize(sameRecord)

        val diffRecord = createRecord(2, "typeB")
        val diffKeyBytes = serializer.serialize(diffRecord)

        Assertions.assertTrue(sameKeyBytes.contentEquals(keyBytes))
        Assertions.assertFalse(diffKeyBytes.contentEquals(keyBytes))
    }
}
