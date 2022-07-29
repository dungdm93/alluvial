package org.apache.iceberg

import org.apache.iceberg.SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP
import org.hamcrest.MatcherAssert
import org.junit.Assert
import org.junit.Test

class TestSquashOperation : TableTestBase(2) {
    companion object {
        val FILE_E = DataFiles.builder(SPEC)
            .withPath("/path/to/data-e.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=4")
            .withRecordCount(1)
            .build()
        val FILE_F = DataFiles.builder(SPEC)
            .withPath("/path/to/data-f.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=4")
            .withRecordCount(1)
            .build()

        val FILE_G = DataFiles.builder(SPEC)
            .withPath("/path/to/data-g.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=4")
            .withRecordCount(1)
            .build()
        val FILE_H = DataFiles.builder(SPEC)
            .withPath("/path/to/data-h.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=4")
            .withRecordCount(1)
            .build()
    }

    private lateinit var ancestorIds: List<Long>

    fun setupAppendOnlyTable() {
        table.newAppend().appendFile(FILE_A).commit()
        table.newAppend().appendFile(FILE_B).commit()
        table.newAppend().appendFile(FILE_C).commit()
        table.newAppend().appendFile(FILE_D).commit()
        table.newAppend().appendFile(FILE_E).commit()
        table.newAppend().appendFile(FILE_F).commit()

        ancestorIds = table.currentAncestorIds().asReversed()
    }

    @Test
    fun testNormalCase() {
        setupAppendOnlyTable()
        val squash = SquashOperation(table.name(), table.ops())
            .squash(2, 4) // (B..D]
        squash.add(FILE_G)
        squash.add(FILE_H)
        squash.commit()

        assertKeepOldSnapshots(table, setOf(1, 2))
        assertSquashSnapshot(table, Pair(2, 4), 7, Pair(listOf(FILE_G, FILE_H), listOf()))
        assertCherrypickSnapshot(table, mapOf(5L to 8L, 6L to 9L))
    }

    private fun assertKeepOldSnapshots(table: Table, keepIds: Set<Long>) {
        val ids = table.currentAncestorIds().toMutableSet()
        @Suppress("ConvertArgumentToSet")
        ids.retainAll(ancestorIds)
        Assert.assertEquals("Should keep un-change snapshot", keepIds, ids)
    }

    @Suppress("SameParameterValue")
    private fun assertSquashSnapshot(
        table: Table,
        squashRange: Pair<Long, Long>, newId: Long,
        newFiles: Pair<List<DataFile>, List<DeleteFile>>
    ) {
        val lowSnapshotId = squashRange.first
        val highSnapshotId = squashRange.second

        val squashedSnapshot = table.ancestorsOf(highSnapshotId)
            .filter { it.snapshotId() > lowSnapshotId }
        val squashedSnapshotIds = squashedSnapshot.map { it.snapshotId() }
        val ids = table.currentAncestorIds().toMutableSet()

        MatcherAssert.assertThat(
            "squashed snapshots MUST not appear in current timeline",
            squashedSnapshotIds.all { it !in ids }
        )
        MatcherAssert.assertThat(
            "new snapshots MUST appear in current timeline",
            ids.contains(newId)
        )

        val newSnapshot = table.snapshot(newId)
        Assert.assertEquals(
            "SQUASH_SNAPSHOT_ID_PROP",
            newSnapshot.summary()[SQUASH_SNAPSHOT_ID_PROP], "(${lowSnapshotId}..${highSnapshotId}]"
        )
        Assert.assertEquals(
            "ORIGINAL_SNAPSHOT_TS_PROP",
            newSnapshot.tsMs(), table.snapshot(highSnapshotId).tsMs()
        )
        Assert.assertEquals(
            "extraMetadata",
            newSnapshot.extraMetadata(), table.snapshot(highSnapshotId).extraMetadata()
        )

        val newDataFiles = newFiles.first
        val newDeleteFiles = newFiles.second
        Assert.assertEquals(
            "squashed snapshots MUST contains those DATA files",
            newDataFiles.map(DataFile::path).toSet(),
            newSnapshot.addedDataFiles(table.io()).map(DataFile::path).toSet()
        )
        Assert.assertEquals(
            "squashed snapshots MUST contains those DATA files",
            newDeleteFiles.map(DeleteFile::path).toSet(),
            newSnapshot.addedDeleteFiles(table.io()).map(DeleteFile::path).toSet()
        )
    }

    private fun assertCherrypickSnapshot(table: Table, idMap: Map<Long, Long>) {
        val io = table.io()
        idMap.forEach { (oldId, newId) ->
            val oldSnapshot = table.snapshot(oldId)
            val newSnapshot = table.snapshot(newId)

            Assert.assertEquals(
                "Snapshot Operation",
                oldSnapshot.operation(), newSnapshot.operation()
            )
            Assert.assertEquals(
                "Snapshot.addedDataFiles",
                oldSnapshot.addedDataFiles(io).map(DataFile::path).toSet(),
                newSnapshot.addedDataFiles(io).map(DataFile::path).toSet()
            )
            Assert.assertEquals(
                "Snapshot.addedDeleteFiles",
                oldSnapshot.addedDeleteFiles(io).map(DeleteFile::path).toSet(),
                newSnapshot.addedDeleteFiles(io).map(DeleteFile::path).toSet()
            )
            Assert.assertEquals(
                "Snapshot.removedDataFiles",
                oldSnapshot.removedDataFiles(io).map(DataFile::path).toSet(),
                newSnapshot.removedDataFiles(io).map(DataFile::path).toSet()
            )
            Assert.assertEquals(
                "Snapshot.removedDeleteFiles",
                oldSnapshot.removedDeleteFiles(io).map(DeleteFile::path).toSet(),
                newSnapshot.removedDeleteFiles(io).map(DeleteFile::path).toSet()
            )

            Assert.assertEquals(
                "SOURCE_SNAPSHOT_ID_PROP",
                newSnapshot.summary()[SOURCE_SNAPSHOT_ID_PROP], oldSnapshot.snapshotId().toString()
            )
            Assert.assertEquals(
                "ORIGINAL_SNAPSHOT_TS_PROP",
                newSnapshot.tsMs(), oldSnapshot.tsMs()
            )
            Assert.assertEquals(
                "extraMetadata",
                newSnapshot.extraMetadata(), oldSnapshot.extraMetadata()
            )
        }
    }
}
