package dev.alluvial.schema.debezium

import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.TestTables
import org.apache.kafka.connect.data.SchemaBuilder.*
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import dev.alluvial.source.kafka.structSchema

internal class TestKafkaSchemaSchemaHandler {
    companion object {
        private const val ANY_INT = 1
        private val KEY_SCHEMA_ORIGIN = structSchema {
            field("id", INT32_SCHEMA)
        }
        private val VALUE_SCHEMA_ORIGIN = structSchema {
            field("id", INT32_SCHEMA)
            field("field1", INT32_SCHEMA)
            field("field2", OPTIONAL_INT32_SCHEMA)
        }
        private val RECORD_ORIGIN = SinkRecord(
            "topic",
            ANY_INT,
            KEY_SCHEMA_ORIGIN,
            KafkaStruct(KEY_SCHEMA_ORIGIN).put("id", ANY_INT),
            VALUE_SCHEMA_ORIGIN,
            KafkaStruct(VALUE_SCHEMA_ORIGIN).put("field1", ANY_INT),
            ANY_INT.toLong()
        )
    }

    @TempDir
    private lateinit var tmpDir: File
    private lateinit var table: Table
    private val formatVersion: Int = 2

    private fun createTable(iSchema: IcebergSchema, schemaHash: Int? = null) {
        table = TestTables.create(tmpDir, "table", iSchema, PartitionSpec.unpartitioned(), formatVersion)
        schemaHash?.let {
            val updater = table.updateProperties()
            updater.set(SCHEMA_HASH_PROP, it.toString())
            updater.commit()
        }
    }

    private fun createOutlet() = IcebergTableOutlet(table.name(), table, SimpleMeterRegistry())

    @BeforeEach
    fun before() {
        assert(tmpDir.deleteRecursively()) { "folder should be deleted" }
    }

    @AfterEach
    fun after() {
        TestTables.clearTables()
        assert(tmpDir.deleteRecursively()) { "folder should be deleted" }
    }

    @Test
    fun recordNullShouldNotMigrate() {
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema())
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)
        val newValueSchema = structSchema { field("fieldSuperNew", INT32_SCHEMA) }
        val record = SinkRecord(
            "topic",
            ANY_INT,
            KEY_SCHEMA_ORIGIN,
            KafkaStruct(KEY_SCHEMA_ORIGIN).put("id", ANY_INT),
            newValueSchema,
            null,
            ANY_INT.toLong()
        )
        Assertions.assertFalse(schemaHandler.shouldMigrate(record))
    }

    @Test
    fun tableNoSchemaHashShouldMigrate() {
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema())
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)
        Assertions.assertTrue(schemaHandler.shouldMigrate(RECORD_ORIGIN))
    }

    @Test
    fun tableHasSchemaHashShouldNotMigrateIfNoNewSchema() {
        val schemaHash = RECORD_ORIGIN.schemaHash()
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema(), schemaHash)
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)
        Assertions.assertFalse(schemaHandler.shouldMigrate(RECORD_ORIGIN))
    }

    @Test
    fun tableHasSchemaHashShouldMigrateIfNewValueSchema() {
        val schemaHash = RECORD_ORIGIN.schemaHash()
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema(), schemaHash)
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        val newValueSchema = structSchema {
            field("id", INT32_SCHEMA)
            field("fieldSuperNew", INT32_SCHEMA)
        }
        val record = SinkRecord(
            "topic",
            ANY_INT,
            KEY_SCHEMA_ORIGIN,
            KafkaStruct(KEY_SCHEMA_ORIGIN).put("id", ANY_INT),
            newValueSchema,
            KafkaStruct(newValueSchema).put("fieldSuperNew", ANY_INT),
            ANY_INT.toLong()
        )
        Assertions.assertTrue(schemaHandler.shouldMigrate(record))
    }

    @Test
    fun tableHasSchemaHashShouldMigrateIfNewKeySchema() {
        val schemaHash = RECORD_ORIGIN.schemaHash()
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema(), schemaHash)
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        val newKeySchema = structSchema {
            field("id", INT32_SCHEMA)
            field("field1", INT32_SCHEMA)
        }
        val record = SinkRecord(
            "topic",
            ANY_INT,
            newKeySchema,
            KafkaStruct(newKeySchema).put("id", ANY_INT).put("field1", ANY_INT),
            VALUE_SCHEMA_ORIGIN,
            KafkaStruct(VALUE_SCHEMA_ORIGIN).put("field1", ANY_INT),
            ANY_INT.toLong()
        )
        Assertions.assertTrue(schemaHandler.shouldMigrate(record))
    }

    @Test
    fun migrateSchemaWhenSchemaHashIsNull() {
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema())
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        Assertions.assertNull(schemaHandler.hashedSchema)

        val valueSchema = structSchema { field("after", VALUE_SCHEMA_ORIGIN) }
        val key = KafkaStruct(KEY_SCHEMA_ORIGIN).put("id", ANY_INT)
        val value = KafkaStruct(valueSchema).put(
            "after", KafkaStruct(VALUE_SCHEMA_ORIGIN)
                .put("field1", ANY_INT)
                .put("id", ANY_INT)
        )
        val record = SinkRecord("topic", ANY_INT, KEY_SCHEMA_ORIGIN, key, valueSchema, value, ANY_INT.toLong())
        schemaHandler.migrateSchema(record)

        val hashed = record.schemaHash()
        Assertions.assertEquals(schemaHandler.hashedSchema, hashed)
        Assertions.assertEquals(table.properties()[SCHEMA_HASH_PROP]?.toInt(), hashed)
    }

    @Test
    fun migrateSchemaWhenSchemaHashNotNull() {
        val hashed = RECORD_ORIGIN.schemaHash()

        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema(), hashed)
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        Assertions.assertEquals(schemaHandler.hashedSchema, hashed)
        Assertions.assertEquals(table.properties()[SCHEMA_HASH_PROP]?.toInt(), hashed)

        // Record with new key schema
        val keySchema = structSchema {
            field("id", INT32_SCHEMA)
            field("field1", INT32_SCHEMA)
        }
        val valueSchema = structSchema { field("after", VALUE_SCHEMA_ORIGIN) }
        val key = KafkaStruct(keySchema)
            .put("id", ANY_INT)
            .put("field1", ANY_INT)
        val value = KafkaStruct(valueSchema).put(
            "after",
            KafkaStruct(VALUE_SCHEMA_ORIGIN)
                .put("field1", ANY_INT)
                .put("id", ANY_INT)
        )
        val record = SinkRecord("topic", ANY_INT, keySchema, key, valueSchema, value, ANY_INT.toLong())
        val newHashed = record.schemaHash()

        schemaHandler.migrateSchema(record)
        Assertions.assertEquals(schemaHandler.hashedSchema, newHashed)
        Assertions.assertEquals(table.properties()[SCHEMA_HASH_PROP]?.toInt(), newHashed)
    }
}
