package org.apache.iceberg

import org.apache.iceberg.DataOperations.DELETE
import org.apache.iceberg.DataOperations.REPLACE
import org.apache.iceberg.SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP
import org.apache.iceberg.common.DynMethods
import org.hamcrest.MatcherAssert
import org.junit.Assert
import org.junit.Test

class TestAlluvialSquashOperation : TableTestBase(2) {
    data class CommitFiles(
        val addedDataFiles: List<DataFile> = emptyList(),
        val addedDeleteFiles: List<DeleteFile> = emptyList(),
        val removedDataFiles: List<DataFile> = emptyList(),
        val removedDeleteFiles: List<DeleteFile> = emptyList(),
    )

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

        private val operationMethod = DynMethods.builder("operation")
            .hiddenImpl(AlluvialSquashOperation::class.java)
            .build()
    }

    private lateinit var ancestorIds: List<Long>

    private fun setupTableForAppendSquash() {
        table.newAppend().appendFile(FILE_A).commit()
        table.newAppend().appendFile(FILE_B).commit()
        table.newAppend().appendFile(FILE_C).commit()
        table.newAppend().appendFile(FILE_D).commit()
        table.newAppend().appendFile(FILE_E).commit()
        table.newAppend().appendFile(FILE_F).commit()

        ancestorIds = table.currentAncestorIds().asReversed()
    }

    @Test
    fun testSquashMiddleCommits() {
        setupTableForAppendSquash()
        val squash = AlluvialSquashOperation(table.name(), table.ops())
            .squash(2, 4) // (B..D]
        squash.add(FILE_G)
        squash.add(FILE_H)
        squash.commit()

        assertKeepOldSnapshots(table, setOf(1, 2))
        assertSquashSnapshot(table, Pair(2, 4), 7, CommitFiles(addedDataFiles = listOf(FILE_G, FILE_H)))
        assertCherrypickSnapshots(table, mapOf(5L to 8L, 6L to 9L))
    }

    @Test
    fun testSquashHeadCommits() {
        setupTableForAppendSquash()
        val squash = AlluvialSquashOperation(table.name(), table.ops())
            .squash(3, 6) // (C..F]
        squash.add(FILE_G)
        squash.add(FILE_H)
        squash.commit()

        assertKeepOldSnapshots(table, setOf(1, 2, 3))
        assertSquashSnapshot(table, Pair(3, 6), 7, CommitFiles(addedDataFiles = listOf(FILE_G, FILE_H)))
        Assert.assertEquals("New Commit is current", table.currentSnapshotId(), 7L)
    }

    @Test
    fun testSquashTailCommits() {
        setupTableForAppendSquash()
        val squash = AlluvialSquashOperation(table.name(), table.ops())
            .squash(null, 4) // (..D]
        squash.add(FILE_G)
        squash.add(FILE_H)
        squash.commit()

        assertKeepOldSnapshots(table, setOf())
        assertSquashSnapshot(table, Pair(null, 4), 7, CommitFiles(addedDataFiles = listOf(FILE_G, FILE_H)))
        assertCherrypickSnapshots(table, mapOf(5L to 8L, 6L to 9L))
    }

    private fun setupTableForNoopSquash() {
        table.newAppend().appendFile(FILE_A).commit() // 1
        table.newAppend().appendFile(FILE_B).commit() // 2
        table.newAppend().appendFile(FILE_C).commit() // 3
        table.newDelete().deleteFile(FILE_B).deleteFile(FILE_C.path()).commit() // 4
        table.newAppend().appendFile(FILE_E).commit() // 5
        table.newOverwrite().deleteFile(FILE_E).commit() // 6
    }

    @Test
    fun testNoop() {
        setupTableForNoopSquash()

        val squash = AlluvialSquashOperation(table.name(), table.ops())
            .squash(1, 4)
        Assert.assertEquals("squash.operation MUST be noop", NOOP, operationMethod.invoke(squash))
        squash.commit()

        Assert.assertEquals(3, table.currentAncestors().count())
        assertCherrypickSnapshots(table, mapOf(5L to 7L, 6L to 8L))
    }

    @Test
    fun testNoopAtTheEnd() {
        setupTableForNoopSquash()

        val squash = AlluvialSquashOperation(table.name(), table.ops())
            .squash(1, 6)
        Assert.assertEquals("squash.operation MUST be noop", NOOP, operationMethod.invoke(squash))
        squash.commit()

        Assert.assertEquals(1, table.currentAncestors().count())
    }

    private fun setupTableForDeleteSquash() {
        table.newAppend().appendFile(FILE_E).commit() // 1
        table.newAppend().appendFile(FILE_F).commit() // 2
        table.newDelete()
            .deleteFile(FILE_E)
            .deleteFile(FILE_F.path())
            .commit() // 3
        table.newRowDelta()
            .addRows(FILE_A)
            .addDeletes(FILE_C2_DELETES)
            .addDeletes(FILE_A2_DELETES)
            .commit() // 4
        table.newFastAppend().appendFile(FILE_B).commit() // 5
        table.newRewrite()
            .rewriteFiles(setOf(FILE_B), setOf(FILE_A2_DELETES), emptySet(), emptySet())
            .commit() // 6
    }

    @Test
    fun testDeleteDataFiles() {
        setupTableForDeleteSquash()

        val squash = AlluvialSquashOperation(table.name(), table.ops())
            .squash(1, 3)
        Assert.assertEquals("squash.operation MUST be noop", DELETE, operationMethod.invoke(squash))
        squash.commit()

        Assert.assertEquals(5, table.currentAncestors().count())
        assertSquashSnapshot(table, Pair(1, 3), 7, CommitFiles(removedDataFiles = listOf(FILE_E)))
        assertCherrypickSnapshots(table, mapOf(4L to 8L, 5L to 9L, 6L to 10L))
    }

    @Test
    fun testDeleteDeleteFiles() {
        setupTableForDeleteSquash()

        val squash = AlluvialSquashOperation(table.name(), table.ops())
            .squash(4, 6)
        Assert.assertEquals("squash.operation MUST be noop", DELETE, operationMethod.invoke(squash))
        squash.commit()

        Assert.assertEquals(5, table.currentAncestors().count())
        assertSquashSnapshot(table, Pair(4, 6), 7, CommitFiles(removedDeleteFiles = listOf(FILE_A2_DELETES)))
    }

    @Test
    fun testRewrite() {
        table.newAppend().appendFile(FILE_A).commit() // 1
        table.newRewrite()
            .rewriteFiles(setOf(FILE_A), emptySet(), setOf(FILE_B), emptySet())
            .commit() // 2
        table.newRewrite()
            .rewriteFiles(setOf(FILE_B), emptySet(), setOf(FILE_C), emptySet())
            .commit() // 3
        table.newRewrite()
            .rewriteFiles(setOf(FILE_C), emptySet(), setOf(FILE_D), emptySet())
            .commit() // 4
        table.newAppend().appendFile(FILE_E).commit() // 5

        val squash = AlluvialSquashOperation(table.name(), table.ops())
            .squash(1, 4)
        squash.add(FILE_F)
        squash.add(FILE_G)
        Assert.assertEquals("squash.operation MUST be noop", REPLACE, operationMethod.invoke(squash))
        squash.commit()

        Assert.assertEquals(3, table.currentAncestors().count())
        assertSquashSnapshot(
            table, Pair(1, 4), 6, CommitFiles(
                addedDataFiles = listOf(FILE_F, FILE_G),
                removedDataFiles = listOf(FILE_A)
            )
        )
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
        squashRange: Pair<Long?, Long>, newId: Long,
        commitFiles: CommitFiles
    ) {
        val lowSnapshotId = squashRange.first
        val highSnapshotId = squashRange.second

        val squashedSnapshot = table.ancestorsOf(highSnapshotId)
            .filterAfter(lowSnapshotId?.let(table::snapshot))
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
            newSnapshot.summary()[SQUASH_SNAPSHOT_ID_PROP], "(${lowSnapshotId ?: ""}..${highSnapshotId}]"
        )
        Assert.assertEquals(
            "ORIGINAL_SNAPSHOT_TS_PROP",
            newSnapshot.originalTimestampMillis(), table.snapshot(highSnapshotId).originalTimestampMillis()
        )
        Assert.assertEquals(
            "extraMetadata",
            newSnapshot.extraMetadata(), table.snapshot(highSnapshotId).extraMetadata()
        )

        Assert.assertEquals(
            "squashed snapshots MUST contains those DATA files",
            commitFiles.addedDataFiles.map(DataFile::path).toSet(),
            newSnapshot.addedDataFiles(table.io()).map(DataFile::path).toSet()
        )
        Assert.assertEquals(
            "squashed snapshots MUST contains those DELETES files",
            commitFiles.addedDeleteFiles.map(DeleteFile::path).toSet(),
            newSnapshot.addedDeleteFiles(table.io()).map(DeleteFile::path).toSet()
        )
        Assert.assertEquals(
            "squashed snapshots MUST removed those DATA files",
            commitFiles.removedDataFiles.map(DataFile::path).toSet(),
            newSnapshot.removedDataFiles(table.io()).map(DataFile::path).toSet()
        )
        Assert.assertEquals(
            "squashed snapshots MUST removed those DELETES files",
            commitFiles.removedDeleteFiles.map(DeleteFile::path).toSet(),
            newSnapshot.removedDeleteFiles(table.io()).map(DeleteFile::path).toSet()
        )
    }

    private fun assertCherrypickSnapshots(table: Table, idMap: Map<Long, Long>) {
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
                newSnapshot.originalTimestampMillis(), oldSnapshot.originalTimestampMillis()
            )
            Assert.assertEquals(
                "extraMetadata",
                newSnapshot.extraMetadata(), oldSnapshot.extraMetadata()
            )
        }
    }
}
