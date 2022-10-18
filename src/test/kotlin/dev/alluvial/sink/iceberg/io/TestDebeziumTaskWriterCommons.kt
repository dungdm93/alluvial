package dev.alluvial.sink.iceberg.io

import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@Suppress("SameParameterValue")
@RunWith(Parameterized::class)
internal class TestDebeziumTaskWriterCommons(
    private val parCol: String?
) : TestDebeziumTaskWriterBase() {
    companion object {
        @JvmStatic
        @Parameters(name = "partition={0}")
        fun parameters(): Collection<String?> {
            return listOf(null, "data")
        }
    }

    private val partitioned = parCol != null

    @Before
    override fun setupTable() {
        createTable(parCol)
        tracker = mockk {
            every { maybeDuplicate(any()) } returns false
            every { update(any()) } answers {}
        }
    }

    @Test
    fun testCdcEvents() {
        // Start the 1st transaction.
        var result = writeRecords(
            sCreateRecord(1, "aaa"),
            sCreateRecord(2, "bbb"),
            sCreateRecord(3, "ccc"),

            // Update <2, 'bbb'> to <2, 'ddd'>
            sUpdateRecord(2, "bbb", "ddd"), // 1 pos-delete and 1 eq-delete.

            // Update <1, 'aaa'> to <1, 'eee'>
            sUpdateRecord(1, "aaa", "eee"), // 1 pos-delete and 1 eq-delete.

            // Insert <4, 'fff'>
            sCreateRecord(4, "fff"),

            // Insert <5, 'ggg'>
            sCreateRecord(5, "ggg"),

            // Delete <3, 'ccc'>
            sDeleteRecord(3, "ccc"), // 1 pos-delete and 1 eq-delete.
        )
        Assert.assertEquals(if (partitioned) 7 else 1, result.dataFiles().size)
        Assert.assertEquals(if (partitioned) 3 else 1, result.deleteFiles().size)

        var expected = arrayOf(
            iRecord(1, "eee"),
            iRecord(2, "ddd"),
            iRecord(4, "fff"),
            iRecord(5, "ggg")
        )
        verify(expected)

        // Start the 2nd transaction.
        result = writeRecords(
            // Update <2, 'ddd'> to <2, 'hhh'>
            sUpdateRecord(2, "ddd", "hhh"), // 1 eq-delete

            // Update <5, 'ggg'> to <5, 'iii'>
            sUpdateRecord(5, "ggg", "iii"), // 1 eq-delete

            // Delete <4, 'fff'>
            sDeleteRecord(4, "fff"), // 1 eq-delete.
        )
        Assert.assertEquals(if (partitioned) 2 else 1, result.dataFiles().size)
        Assert.assertEquals(if (partitioned) 3 else 1, result.deleteFiles().size)

        expected = arrayOf(
            iRecord(1, "eee"),
            iRecord(2, "hhh"),
            iRecord(5, "iii"),
        )
        verify(expected)
    }

    private fun sCreateRecord(id: Int, data: String): SinkRecord {
        val row = rowFor(id, data)
        return recordFor("c", row, null, row, null)
    }

    private fun sUpdateRecord(id: Int, before: String, after: String): SinkRecord {
        val beforeRow = rowFor(id, before)
        val afterRow = rowFor(id, after)
        return recordFor("u", afterRow, beforeRow, afterRow, null)
    }

    private fun sDeleteRecord(id: Int, data: String): SinkRecord {
        val row = rowFor(id, data)
        return recordFor("d", row, row, null, null)
    }
}
