package dev.alluvial.dedupe.rocksdb

import dev.alluvial.dedupe.backend.rocksdb.BasicRecordSerializer
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.structSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestBasicRecordSerializer {
    @Test
    fun testSerialize() {
        val schema = structSchema {
            field("id", Schema.INT32_SCHEMA)
            field("type", Schema.STRING_SCHEMA)
        }
        val value = KafkaStruct(schema).put("id", 1).put("type", "basic")
        val record = SinkRecord("topic", 1, schema, value, schema, value, 0)
        val ids = record.keySchema().fields().map { it.name() }
        val serializer = BasicRecordSerializer(ids)
        val recordKeyBytes = serializer.serialize(record)
        Assertions.assertEquals("1-basic", recordKeyBytes.decodeToString())
    }
}
