package dev.alluvial.stream.debezium

import dev.alluvial.sink.iceberg.type.KafkaStruct
import io.mockk.every
import io.mockk.mockk
import org.apache.iceberg.BROKER_OFFSETS_PROP
import org.apache.iceberg.SOURCE_TIMESTAMP_PROP
import org.apache.iceberg.SOURCE_WAL_POSITION_PROP
import org.apache.iceberg.Snapshot
import org.apache.iceberg.Table
import org.apache.iceberg.mapper
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

internal class TestRecordTracker {
    private val summary = mapOf(
        SOURCE_TIMESTAMP_PROP to currentTs.toString(),
        SOURCE_WAL_POSITION_PROP to mapper.writeValueAsString(PostgresWALPosition(currentWALPosition)),
        BROKER_OFFSETS_PROP to mapper.writeValueAsString(mapOf(1 to currentOffsets))
    )
    private val table: Table = mockk {
        val snapshot = mockk<Snapshot> {
            every { summary() } returns summary
        }
        every { currentSnapshot() } returns snapshot
    }
    private val tracker = RecordTracker.create("postgresql", table)

    @Test
    fun testUpdate() {
        val s = tracker.buildSummary()
        Assertions.assertEquals(summary, s)

        val tombstoneRecord = mockk<SinkRecord> {
            every { value() } returns null
            every { kafkaPartition() } returns 1
            every { kafkaOffset() } returns 5L
        }
        tracker.update(tombstoneRecord)
        Assertions.assertEquals(
            summary + (BROKER_OFFSETS_PROP to mapper.writeValueAsString(mapOf(1 to 6L))),
            tracker.buildSummary()
        )

        val record = mockk<SinkRecord> {
            every { kafkaPartition() } returns 1
            every { kafkaOffset() } returns 6L
            val source: KafkaStruct = mockk {
                val struct = this
                val tsMs = 666666666L
                val lsn = 66666L
                every { struct.getInt64("ts_ms") } returns tsMs
                every { struct.get("ts_ms") } returns tsMs
                every { struct.getInt64("lsn") } returns lsn
                every { struct.get("lsn") } returns lsn
            }
            every { value() } returns mockk<KafkaStruct> {
                val struct = this
                every { struct.getStruct("source") } returns source
                every { struct.get("source") } returns source
            }
        }
        tracker.update(record)
        Assertions.assertEquals(
            mapOf(
                SOURCE_TIMESTAMP_PROP to "666666666",
                SOURCE_WAL_POSITION_PROP to mapper.writeValueAsString(PostgresWALPosition(66666)),
                BROKER_OFFSETS_PROP to mapper.writeValueAsString(mapOf(1 to 7L))
            ),
            tracker.buildSummary()
        )
    }

    @ParameterizedTest
    @MethodSource("provideSinkRecord")
    fun testMaybeDuplicate(record: SinkRecord, result: Boolean) {
        if (result)
            Assertions.assertTrue(tracker.maybeDuplicate(record))
        else
            Assertions.assertFalse(tracker.maybeDuplicate(record))
    }

    companion object {
        private const val currentTs = 555555555L
        private const val currentWALPosition = 55555L
        private const val currentOffsets = 5L

        @JvmStatic
        fun provideSinkRecord(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(recordOf(1, Long.MAX_VALUE), true),
                Arguments.of(recordOf(Long.MAX_VALUE, 1), true),
                Arguments.of(recordOf(currentTs, currentWALPosition - 1), true),
                Arguments.of(recordOf(currentTs, currentWALPosition), true),
                Arguments.of(recordOf(currentTs, currentWALPosition + 1), false),
                Arguments.of(recordOf(currentTs + 1, currentWALPosition + 1), false),
            )
        }

        private fun recordOf(sourceTs: Long, lsn: Long): SinkRecord {
            return mockk {
                val record = this
                val source: KafkaStruct = mockk {
                    val struct = this
                    every { struct.getInt64("ts_ms") } returns sourceTs
                    every { struct.get("ts_ms") } returns sourceTs
                    every { struct.getInt64("lsn") } returns lsn
                    every { struct.get("lsn") } returns lsn
                }
                every { record.value() } returns mockk<KafkaStruct> {
                    val struct = this
                    every { struct.getStruct("source") } returns source
                    every { struct.get("source") } returns source
                }
                every { record.toString() } returns "SinkRecord(ts_ms=$sourceTs, lsn=$lsn)"
            }
        }
    }
}
