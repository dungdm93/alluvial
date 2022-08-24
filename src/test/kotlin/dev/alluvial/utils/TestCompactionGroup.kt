package dev.alluvial.utils

import org.apache.iceberg.Snapshot
import org.apache.iceberg.catalog.TableIdentifier
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock

internal class TestCompactionGroup {
    private val tableId = TableIdentifier.of("foo", "bar")
    lateinit var snapshots: List<Snapshot>

    private fun mockSnapshot(id: Long, parentId: Long?, seqNum: Long): Snapshot {
        return mock {
            on { snapshotId() } doReturn id
            on { parentId() } doReturn parentId
            on { sequenceNumber() } doReturn seqNum
        }
    }

    @BeforeEach
    fun setup() {
        snapshots = buildList<Snapshot> {
            for (i in 2L..10L) {
                add(mockSnapshot(i, i - 1, i))
            }
        }.reversed()
    }

    @Test
    fun testSimple() {
        val cgs = CompactionGroup.fromSnapshots(tableId, snapshots) {
            when {
                it.snapshotId() <= 3 -> "key-3"
                it.snapshotId() <= 5 -> "key-5"
                it.snapshotId() <= 10 -> "key-10"
                else -> "+inf"
            }
        }.toList()
        Assertions.assertEquals(3, cgs.size)

        val firstCG = cgs[0]
        Assertions.assertEquals(5, firstCG.size)
        Assertions.assertEquals(5, firstCG.lowSequenceNumber)
        Assertions.assertEquals(10, firstCG.highSequenceNumber)
        Assertions.assertEquals("key-10", firstCG.key)

        val secondCG = cgs[1]
        Assertions.assertEquals(2, secondCG.size)
        Assertions.assertEquals(3, secondCG.lowSequenceNumber)
        Assertions.assertEquals(5, secondCG.highSequenceNumber)
        Assertions.assertEquals("key-5", secondCG.key)

        val thirdCG = cgs[2]
        Assertions.assertEquals(2, thirdCG.size)
        Assertions.assertNull(thirdCG.lowSequenceNumber)
        Assertions.assertEquals(3, thirdCG.highSequenceNumber)
        Assertions.assertEquals("key-3", thirdCG.key)
    }


    @Test
    fun testFirstElementInItOwnGroup() {
        val cgs = CompactionGroup.fromSnapshots(tableId, snapshots) {
            when {
                it.snapshotId() <= 2 -> "key-2"
                it.snapshotId() <= 10 -> "key-10"
                else -> "+inf"
            }
        }.toList()
        Assertions.assertEquals(2, cgs.size)

        val firstCG = cgs[0]
        Assertions.assertEquals(8, firstCG.size)
        Assertions.assertEquals(2, firstCG.lowSequenceNumber)
        Assertions.assertEquals(10, firstCG.highSequenceNumber)
        Assertions.assertEquals("key-10", firstCG.key)

        val secondCG = cgs[1]
        Assertions.assertEquals(1, secondCG.size)
        Assertions.assertNull(secondCG.lowSequenceNumber)
        Assertions.assertEquals(2, secondCG.highSequenceNumber)
        Assertions.assertEquals("key-2", secondCG.key)
    }
}
