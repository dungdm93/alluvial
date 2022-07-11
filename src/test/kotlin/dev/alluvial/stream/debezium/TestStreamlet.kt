package dev.alluvial.stream.debezium

import dev.alluvial.runtime.StreamConfig
import dev.alluvial.schema.debezium.KafkaSchemaSchemaHandler
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.sink.iceberg.type.IcebergSchema
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
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.AfterEach
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
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.io.File

internal class TestStreamlet {
    companion object {
        private const val topic = "topic"
        private val streamConfig = StreamConfig("iceberg")
        private val registry = SimpleMeterRegistry()
        private val defaultValueSchema = structSchema {
            field("before", structSchema { field("id", Schema.INT32_SCHEMA) })
            field("after", structSchema { field("id", Schema.INT32_SCHEMA) })
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

    private fun createOutlet() = IcebergTableOutlet(table.name(), table, registry)

    private fun createTable(iSchema: IcebergSchema) {
        table = TestTables.create(tmpDir, "table", iSchema, PartitionSpec.unpartitioned(), 2)
    }

    private fun record(valueSchema: KafkaSchema? = null, value: KafkaStruct? = null, offset: Long = 0) = SinkRecord(
        topic,
        1,
        defaultKeySchema,
        defaultKey,
        valueSchema,
        value,
        offset,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME
    )


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
    fun testCaptureChangesEncounterTombstoneAtStart() {
        val tombstoneRecord = record()
        val createRecord = record(
            defaultValueSchema,
            KafkaStruct(defaultValueSchema).put(
                "before",
                KafkaStruct(structSchema { field("id", Schema.INT32_SCHEMA) }).put("id", 1)
            ).put(
                "after",
                KafkaStruct(structSchema { field("id", Schema.INT32_SCHEMA) }).put("id", 2)
            ).put("op", "c")
        )
        val inlet: KafkaTopicInlet = mock()
        whenever(inlet.read())
            .doReturn(tombstoneRecord)
            .thenReturn(createRecord)
            .thenReturn(null)

        createTable(defaultValueSchema.toIcebergSchema())
        val outlet = spy(createOutlet())
        val handler = KafkaSchemaSchemaHandler(outlet)

        val streamlet = spy(DebeziumStreamlet("streamlet", inlet, outlet, handler, streamConfig, registry))
        val shouldRunAnswer = listOf(
            true, // Start
            true, // Streamlet has read 2 records, then got null
            true, // Streamlet still run but got null record
            false
        ).iterator()
        // Partial mock. Refer: https://groups.google.com/g/mockito/c/9WUvkhZUy90
        doAnswer { shouldRunAnswer.next() }.whenever(streamlet).shouldRun()
        streamlet.run()

        // Only one valid record should be written
        verify(outlet, times(1)).write(any())

        table.refresh()
        expectThat(table.currentSnapshot().summary()[TOTAL_RECORDS_PROP]).isEqualTo("1")
    }
}
