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

    private fun createTable(iSchema: IcebergSchema, schemaVersion: String? = null) {
        table = TestTables.create(tmpDir, "table", iSchema, PartitionSpec.unpartitioned(), formatVersion)
        schemaVersion?.let {
            val updater = table.updateProperties()
            updater.set(SCHEMA_VERSION_PROP, it)
            updater.commit()
        }
    }

    private fun createOutlet() = IcebergTableOutlet(table.name(), table, null, SimpleMeterRegistry())

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
    fun tableNoSchemaVersionShouldMigrate() {
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema())
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)
        Assertions.assertTrue(schemaHandler.shouldMigrate(RECORD_ORIGIN))
    }

    @Test
    fun tableHasSchemaVersionShouldNotMigrateIfNoNewSchema() {
        val schemaVersion = RECORD_ORIGIN.schemaVersion()
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema(), schemaVersion)
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)
        Assertions.assertFalse(schemaHandler.shouldMigrate(RECORD_ORIGIN))
    }

    @Test
    fun tableHasSchemaVersionShouldMigrateIfNewValueSchema() {
        val schemaVersion = RECORD_ORIGIN.schemaVersion()
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema(), schemaVersion)
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        val newValueSchema = structSchema(2) {
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
    fun tableHasSchemaVersionShouldMigrateIfNewKeySchema() {
        val schemaVersion = RECORD_ORIGIN.schemaVersion()
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema(), schemaVersion)
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        val newKeySchema = structSchema(2) {
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
    fun migrateSchemaWhenSchemaVersionIsNull() {
        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema())
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        Assertions.assertNull(schemaHandler.schemaVersion)

        val valueSchema = structSchema { field("after", VALUE_SCHEMA_ORIGIN) }
        val key = KafkaStruct(KEY_SCHEMA_ORIGIN).put("id", ANY_INT)
        val value = KafkaStruct(valueSchema).put(
            "after", KafkaStruct(VALUE_SCHEMA_ORIGIN)
                .put("field1", ANY_INT)
                .put("id", ANY_INT)
        )
        val record = SinkRecord("topic", ANY_INT, KEY_SCHEMA_ORIGIN, key, valueSchema, value, ANY_INT.toLong())
        schemaHandler.migrateSchema(record)

        val version = record.schemaVersion()
        Assertions.assertEquals(schemaHandler.schemaVersion, version)
        Assertions.assertEquals(table.properties()[SCHEMA_VERSION_PROP], version)
    }

    @Test
    fun migrateSchemaWhenSchemaVersionNotNull() {
        val version = RECORD_ORIGIN.schemaVersion()

        createTable(VALUE_SCHEMA_ORIGIN.toIcebergSchema(), version)
        val outlet = createOutlet()
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        Assertions.assertEquals(schemaHandler.schemaVersion, version)
        Assertions.assertEquals(table.properties()[SCHEMA_VERSION_PROP], version)

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
        val newVersion = record.schemaVersion()

        schemaHandler.migrateSchema(record)
        Assertions.assertEquals(schemaHandler.schemaVersion, newVersion)
        Assertions.assertEquals(table.properties()[SCHEMA_VERSION_PROP], newVersion)
    }
}
