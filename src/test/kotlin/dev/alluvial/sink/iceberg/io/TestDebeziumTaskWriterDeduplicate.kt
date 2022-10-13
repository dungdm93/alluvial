package dev.alluvial.sink.iceberg.io

import dev.alluvial.stream.debezium.RecordTracker
import org.apache.iceberg.PartitionSpec
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Before
import org.junit.Test

@Suppress("SameParameterValue")
internal open class TestDebeziumTaskWriterDeduplicate : TestDebeziumTaskWriterBase() {
    @Before
    override fun setupTable() {
        createTable(PartitionSpec.unpartitioned())
        tracker = RecordTracker.create("postgresql", table)
    }

    private fun sReadRecord(id: Int, data: String, sourceTs: Long, lsn: Long): SinkRecord {
        val row = rowFor(id, data)
        val source = sourceFor(sourceTs, lsn)
        return recordFor("r", row, null, row, source)
    }

    private fun sCreateRecord(id: Int, data: String, sourceTs: Long, lsn: Long): SinkRecord {
        val row = rowFor(id, data)
        val source = sourceFor(sourceTs, lsn)
        return recordFor("c", row, null, row, source)
    }

    private fun sUpdateRecord(id: Int, before: String, after: String, sourceTs: Long, lsn: Long): SinkRecord {
        val beforeRow = rowFor(id, before)
        val afterRow = rowFor(id, after)
        val source = sourceFor(sourceTs, lsn)
        return recordFor("u", afterRow, beforeRow, afterRow, source)
    }

    private fun sDeleteRecord(id: Int, data: String, sourceTs: Long, lsn: Long): SinkRecord {
        val row = rowFor(id, data)
        val source = sourceFor(sourceTs, lsn)
        return recordFor("d", row, row, null, source)
    }

    @Test
    fun testReadThenCreate() {
        // All read records have the same source_timestamp and wal_position
        writeRecords(
            sReadRecord(4, "444", 555555555, 555),
            sReadRecord(5, "555", 555555555, 555),
            sReadRecord(6, "666", 555555555, 555),
        )

        // CREATE record have source_timestamp < snapshot but wal_position > snapshot
        writeRecords(
            sCreateRecord(5, "555", 444444444, 666), // <= dup with snapshot
            sCreateRecord(7, "777", 444444444, 667),
        )

        val expected = arrayOf(
            iRecord(4, "444"),
            iRecord(5, "555"),
            iRecord(6, "666"),
            iRecord(7, "777"),
        )
        verify(expected)
    }

    @Test
    fun testDebeziumBatchRetry() {
        writeRecords(
            sCreateRecord(5, "555", 555555555, 555),
            sUpdateRecord(5, "555", "556", 555555555, 566),
            sUpdateRecord(5, "556", "557", 555555555, 577),
            sCreateRecord(6, "666", 555555555, 666),
        )
        writeRecords(
            sCreateRecord(5, "555", 555555555, 555),
            sUpdateRecord(5, "555", "556", 555555555, 566),
            sUpdateRecord(5, "556", "557", 555555555, 577),
            sCreateRecord(7, "777", 555555555, 777),
        )

        val expected = arrayOf(
            iRecord(5, "557"),
            iRecord(6, "666"),
            iRecord(7, "777"),
        )
        verify(expected)
    }
}
