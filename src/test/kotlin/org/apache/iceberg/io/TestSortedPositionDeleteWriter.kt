package org.apache.iceberg.io

import dev.alluvial.sink.iceberg.io.PathOffset
import io.mockk.every
import io.mockk.mockk
import org.apache.iceberg.deletes.PositionDelete
import org.apache.iceberg.deletes.PositionDeleteWriter
import org.apache.iceberg.encryption.EncryptedOutputFile
import org.apache.iceberg.types.Comparators
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TestSortedPositionDeleteWriter {
    private val persistedRecords = mutableListOf<PathOffset>()
    private lateinit var writer: SortedPositionDeleteWriter<Any>
    private val pathOffsetComparator = object : Comparator<PathOffset> {
        private val pathCom = Comparators.charSequences()
        override fun compare(o1: PathOffset, o2: PathOffset): Int {
            val res = pathCom.compare(o1.path(), o2.path())
            return if (res != 0)
                res else
                o1.offset().compareTo(o2.offset())
        }
    }

    private fun <T> createRollingPosDelWriter(): RollingPositionDeleteWriter<T> {
        val io = mockk<FileIO>(relaxed = true)
        val fileFactory = mockk<OutputFileFactory> {
            val outputFile = mockk<EncryptedOutputFile> {
                every { encryptingOutputFile() } returns mockk()
            }
            every { newOutputFile() } returns outputFile
            every { newOutputFile(any()) } returns outputFile
            every { newOutputFile(any(), any()) } returns outputFile
        }
        val writerFactory = mockk<FileWriterFactory<T>> {
            val writer = mockk<PositionDeleteWriter<T>> {
                every { write(any<PositionDelete<T>>()) } answers {
                    val delete = firstArg<PositionDelete<*>>()
                    persistedRecords.add(delete.toPathOffset())
                }
                every { close() } answers {}
                every { result() } returns DeleteWriteResult(emptyList())
            }
            every { newPositionDeleteWriter(any(), any(), any()) } returns writer
        }
        return RollingPositionDeleteWriter(writerFactory, fileFactory, io, 1000000, null, null)
    }

    @BeforeEach
    fun setupWriter() {
        val w = createRollingPosDelWriter<Any>()
        writer = SortedPositionDeleteWriter(w)
    }

    private fun PositionDelete<*>.toPathOffset(): PathOffset {
        return PathOffset.of(this.path(), this.pos())
    }

    private fun assertWriteDeletesInOrder() {
        var previous: PathOffset? = null
        for (po in persistedRecords) {
            if (previous == null) {
                previous = po
                continue
            }
            Assertions.assertTrue(pathOffsetComparator.compare(previous, po) <= 0) {
                "PathOffset is out of order previous=$previous, current=$po"
            }
            previous = po
        }
    }

    private fun assertWriteDeletesEquals(deletes: List<PositionDelete<*>>) {
        val originalPathOffset = deletes.mapTo(mutableListOf()) { it.toPathOffset() }
        originalPathOffset.sortWith(pathOffsetComparator)
        Assertions.assertIterableEquals(originalPathOffset, persistedRecords)
    }

    @Test
    fun testRowsIsSorted() {
        val deletes = listOf(
            PositionDelete.create<Any>().set("aaa", 1, null),
            PositionDelete.create<Any>().set("aaa", 3, null),
            PositionDelete.create<Any>().set("aaa", 7, null),
            PositionDelete.create<Any>().set("bbb", 2, null),
            PositionDelete.create<Any>().set("bbb", 5, null),
            PositionDelete.create<Any>().set("ccc", 4, null),
        )

        writer.write(deletes.shuffled())
        writer.close()

        assertWriteDeletesInOrder()
        assertWriteDeletesEquals(deletes)
    }
}
