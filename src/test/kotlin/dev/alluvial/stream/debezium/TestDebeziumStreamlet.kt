package dev.alluvial.stream.debezium

import dev.alluvial.runtime.StreamConfig
import dev.alluvial.schema.debezium.KafkaSchemaSchemaHandler
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import dev.alluvial.source.kafka.KafkaTopicInlet
import dev.alluvial.source.kafka.structSchema
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP
import org.apache.iceberg.Table
import org.apache.iceberg.TestTables
import org.apache.iceberg.data.IcebergGenerics
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.File

internal class TestDebeziumStreamlet {
    companion object {
        private const val topic = "topic"
        private val streamConfig = StreamConfig("iceberg")
        private val registry = SimpleMeterRegistry()
        private val recordSchema = structSchema {
            field("id", Schema.INT32_SCHEMA)
        }
        private val sourceInfoSchema = structSchema {
            field("ts_ms", Schema.INT64_SCHEMA)
        }
        private val defaultValueSchema = structSchema {
            field("before", recordSchema)
            field("after", recordSchema)
            field("source", sourceInfoSchema)
            field("op", Schema.STRING_SCHEMA)
        }
        private val defaultKeySchema = structSchema {
            field("id", SchemaBuilder.INT32_SCHEMA)
        }
        private val defaultKey = KafkaStruct(defaultKeySchema).put("id", 1)
    }

    @TempDir
    private lateinit var tmpDir: File
    private lateinit var table: Table

    private fun record(
        keySchema: Schema? = defaultKeySchema, key: KafkaStruct? = defaultKey,
        valueSchema: KafkaSchema? = null, value: KafkaStruct? = null,
        offset: Long = 0
    ): SinkRecord {
        val ts = System.currentTimeMillis()
        if (value != null) {
            val source = value.getStruct("source")
            if (source == null) {
                val s = KafkaStruct(sourceInfoSchema)
                    .put("ts_ms", ts)
                value.put("source", s)
            }
        }
        return SinkRecord(
            topic, 1,
            keySchema, key,
            valueSchema, value,
            offset, System.currentTimeMillis(), TimestampType.CREATE_TIME
        )
    }

    @BeforeEach
    fun before() {
        assert(tmpDir.deleteRecursively()) { "folder should be deleted" }
        val iSchema = defaultValueSchema.toIcebergSchema()
        table = TestTables.create(tmpDir, "table", iSchema, PartitionSpec.unpartitioned(), 2)
    }

    @AfterEach
    fun after() {
        TestTables.clearTables()
        assert(tmpDir.deleteRecursively()) { "folder should be deleted" }
    }

    private fun mockInlet(record: SinkRecord?, vararg records: SinkRecord?): KafkaTopicInlet {
        return mock {
            on { read() }.doReturn(record, *records)
        }
    }

    private fun spyOutlet(): IcebergTableOutlet {
        return spy(IcebergTableOutlet(table.name(), table, registry))
    }

    private fun spyStreamlet(
        inlet: KafkaTopicInlet,
        outlet: IcebergTableOutlet,
        config: StreamConfig,
        shouldRuns: List<Boolean>,
    ): DebeziumStreamlet {
        val handler = KafkaSchemaSchemaHandler(outlet)
        val iter = shouldRuns.iterator()
        val streamlet = spy(DebeziumStreamlet("streamlet", inlet, outlet, handler, config, registry))
        // Partial mock. Refer: https://groups.google.com/g/mockito/c/9WUvkhZUy90
        doAnswer { iter.next() }.whenever(streamlet).shouldRun()
        return streamlet
    }

    @Test
    fun testCaptureChangesEncounterTombstoneAtStart() {
        val tombstoneRecord = record()
        val createRecord = record(
            valueSchema = defaultValueSchema,
            value = KafkaStruct(defaultValueSchema)
                .put("before", KafkaStruct(recordSchema).put("id", 1))
                .put("after", KafkaStruct(recordSchema).put("id", 2))
                .put("op", "c")
        )
        val inlet = mockInlet(tombstoneRecord, createRecord, null)
        val outlet = spyOutlet()

        val shouldRunAnswer = listOf(
            true, // Start
            true, // Streamlet has read 2 records, then got null
            true, // Streamlet still run but got null record
            false
        )
        val streamlet = spyStreamlet(inlet, outlet, streamConfig, shouldRunAnswer)
        streamlet.run()

        // Only one valid record should be written
        verify(outlet, times(1)).write(any())

        table.refresh()
        Assertions.assertEquals(table.currentSnapshot().summary()[TOTAL_RECORDS_PROP], "1")

        val writtenRecords = IcebergGenerics.read(table).select("id").build().toList()
        Assertions.assertEquals(writtenRecords.size, 1)
        Assertions.assertEquals(writtenRecords[0].getField("id"), 2)
    }

    @Test
    fun captureTruncateEvent() {
        val createRecord = record(
            valueSchema = defaultValueSchema,
            value = KafkaStruct(defaultValueSchema)
                .put("before", KafkaStruct(recordSchema).put("id", 1))
                .put("after", KafkaStruct(recordSchema).put("id", 2))
                .put("op", "c")
        )
        val truncateRecord = record(
            keySchema = null,
            key = null,
            valueSchema = defaultValueSchema,
            value = KafkaStruct(defaultValueSchema).put("op", "t")
        )
        val inlet = mockInlet(createRecord, truncateRecord, null)
        val outlet = spyOutlet()

        val shouldRunAnswer = listOf(true, true, true, false)
        val streamlet = spyStreamlet(inlet, outlet, streamConfig.copy(commitBatchSize = 1), shouldRunAnswer)
        streamlet.run()

        // Only "create" record should be written
        verify(outlet, times(2)).write(any())
        verify(outlet, times(1)).commit(any())

        table.refresh()
        Assertions.assertEquals(table.currentSnapshot().summary()[TOTAL_RECORDS_PROP], "0")

        val writtenRecords = IcebergGenerics.read(table).select().build().toList()
        Assertions.assertEquals(writtenRecords.size, 0)
    }

    @Test
    fun captureTruncateEventAtBeginning() {
        val truncateRecord = record(
            keySchema = null,
            key = null,
            valueSchema = defaultValueSchema,
            value = KafkaStruct(defaultValueSchema).put("op", "t")
        )
        val inlet = mockInlet(truncateRecord, null)
        val outlet = spyOutlet()

        val shouldRunAnswer = listOf(true, false)
        val streamlet = spyStreamlet(inlet, outlet, streamConfig, shouldRunAnswer)
        streamlet.run()

        // Only "create" record should be written
        verify(outlet, times(1)).write(any())
        verify(outlet, times(0)).commit(any())

        table.refresh()
        Assertions.assertEquals(table.currentSnapshot().summary()[TOTAL_RECORDS_PROP], "0")

        val writtenRecords = IcebergGenerics.read(table).select().build().toList()
        Assertions.assertEquals(writtenRecords.size, 0)
    }
}
