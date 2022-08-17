package org.apache.iceberg

import dev.alluvial.sink.iceberg.io.GenericReader
import org.apache.iceberg.FileContent.EQUALITY_DELETES
import org.apache.iceberg.FileContent.POSITION_DELETES
import org.apache.iceberg.TestTables.TestTable
import org.apache.iceberg.data.FileHelpers.writeDataFile
import org.apache.iceberg.data.FileHelpers.writeDeleteFile
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.types.Types.IntegerType
import org.apache.iceberg.types.Types.NestedField.required
import org.apache.iceberg.types.Types.StringType
import org.apache.iceberg.util.Pair
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.io.TempDir
import java.io.File

class TestCompactSnapshots {
    private val schema = Schema(
        required(1, "id", IntegerType.get()),
        required(2, "data", StringType.get())
    )
    private val eqDelSchema = schema.select("id")
    private val posDelSchema = POS_DELETE_SCHEMA
    private val spec = PartitionSpec.builderFor(schema).build()
    private val record = GenericRecord.create(schema)
    private val eqDelRecord = GenericRecord.create(eqDelSchema)
    private val posDelRecord = GenericRecord.create(posDelSchema)

    @TempDir
    private lateinit var tableDir: File
    private lateinit var table: TestTable

    @BeforeEach
    fun setupTable() {
        tableDir.delete()
        table = TestTables.create(tableDir, "test", schema, spec, 2)
    }

    @AfterEach
    fun cleanupTables() {
        TestTables.clearTables()
    }

    private fun setupTableForNormalCompact() {
        val file1 = writeDataFile(
            table, Files.localOutput(table.location() + "/data/data_a.parquet"), listOf(
                record.copy("id", 1, "data", "first"),
                record.copy("id", 2, "data", "second"),
                record.copy("id", 3, "data", "third"),
            )
        )
        table.newAppend() // #1
            .appendFile(file1)
            .commit()

        val file2 = writeDataFile(
            table, Files.localOutput(table.location() + "/data/data_b.parquet"), listOf(
                record.copy("id", 4, "data", "fourth"),
                record.copy("id", 5, "data", "fifth"),
                record.copy("id", 6, "data", "sixth"),
                record.copy("id", 7, "data", "seventh"),
            )
        )
        table.newAppend() // #2
            .appendFile(file2)
            .commit()

        val file3 = writeDeleteFile(
            table, Files.localOutput(table.location() + "/data/eq_deletes_ab.parquet"), listOf(
                eqDelRecord.copy("id", 4),
                eqDelRecord.copy("id", 6),
                eqDelRecord.copy("id", 1),
            ), eqDelSchema
        )
        val file4 = writeDataFile(
            table, Files.localOutput(table.location() + "/data/data_c.parquet"), listOf(
                record.copy("id", 10, "data", "ten"),
            )
        )
        val file5 = writeDeleteFile(
            table, Files.localOutput(table.location() + "/data/pos_deletes_bc.parquet"), listOf(
                Pair.of(file4.path(), 0L),
                Pair.of(file2.path(), 3L),
            )
        )
        table.newRowDelta() // #3
            .addDeletes(file3)
            .addRows(file4)
            .addDeletes(file5.first())
            .validateDeletedFiles()
            .validateDataFilesExist(file5.second())
            .commit()

        val file6 = writeDataFile(
            table, Files.localOutput(table.location() + "/data/data_d.parquet"), listOf(
                record.copy("id", 6, "data", "six"),
                record.copy("id", 7, "data", "seven"),
            )
        )
        table.newAppend() // #4
            .appendFile(file6)
            .commit()
    }

    @Test
    fun testSimpleCompact() {
        setupTableForNormalCompact()
        val compaction = CompactSnapshots(table, 1, 3)
        compaction.execute()

        Assertions.assertEquals(6, table.snapshots().count())
        Assertions.assertEquals(3, table.currentAncestors().count())
        assertNewSnapshot(
            5,
            listOf(record.copy("id", 5, "data", "fifth")),
            listOf(
                eqDelRecord.copy("id", 1),
                eqDelRecord.copy("id", 4),
                eqDelRecord.copy("id", 6),
            ),
            emptyList()
        )
        assertDataConsistent(4, 6)
    }

    @Test
    fun testSimpleCompactFromRoot() {
        setupTableForNormalCompact()
        val compaction = CompactSnapshots(table, null, 3)
        compaction.execute()

        Assertions.assertEquals(6, table.snapshots().count())
        Assertions.assertEquals(2, table.currentAncestors().count())
        Assertions.assertNull(table.snapshot(5L).parentId())
        assertNewSnapshot(
            5,
            listOf(
                record.copy("id", 2, "data", "second"),
                record.copy("id", 3, "data", "third"),
                record.copy("id", 5, "data", "fifth"),
            ),
            emptyList(),
            emptyList()
        )
        assertDataConsistent(4, 6)
    }

    @Test
    fun testNotAbleToSquash() {
        setupTableForNormalCompact()
        val compaction = CompactSnapshots(table, 2, 4)
        val exception = assertThrows<ValidationException>(compaction::execute)
        Assertions.assertEquals(
            exception.message,
            "Snapshot 3 contains POSITION_DELETES file reference to out of CompactionGroup"
        )
    }

    @Test
    fun testNotAbleToCherrypick() {
        setupTableForNormalCompact()
        val compaction = CompactSnapshots(table, null, 2)
        val exception = assertThrows<ValidationException>(compaction::execute)
        Assertions.assertEquals(
            exception.message,
            "Cannot cherry-pick snapshot 3: Found POSITION_DELETES reference to the dead files"
        )
    }

    private fun setupTableWithRemovedFiles() {
        val file1 = writeDataFile(
            table, Files.localOutput(table.location() + "/data/data_a.parquet"), listOf(
                record.copy("id", 1, "data", "first"),
                record.copy("id", 2, "data", "second"),
                record.copy("id", 3, "data", "third"),
            )
        )
        table.newAppend() // #1
            .appendFile(file1)
            .commit()

        val file2 = writeDataFile(
            table, Files.localOutput(table.location() + "/data/data_b.parquet"), listOf(
                record.copy("id", 4, "data", "fourth"),
                record.copy("id", 5, "data", "fifth"),
                record.copy("id", 6, "data", "sixth"),
                record.copy("id", 7, "data", "seventh"),
            )
        )
        table.newAppend() // #2
            .appendFile(file2)
            .commit()

        table.newDelete() // #3
            .deleteFile(file1)
            .commit()

        val file3 = writeDeleteFile(
            table, Files.localOutput(table.location() + "/data/eq_deletes_1.parquet"), listOf(
                eqDelRecord.copy("id", 4),
                eqDelRecord.copy("id", 1),
            ), eqDelSchema
        )
        table.newRowDelta() // #4
            .addDeletes(file3)
            .commit()

        val file4 = writeDeleteFile(
            table, Files.localOutput(table.location() + "/data/eq_deletes_2.parquet"), listOf(
                eqDelRecord.copy("id", 4),
                eqDelRecord.copy("id", 6),
            ), eqDelSchema
        )
        table.newRowDelta() // #5
            .addDeletes(file4)
            .commit()

        val file6 = writeDataFile(
            table, Files.localOutput(table.location() + "/data/data_d.parquet"), listOf(
                record.copy("id", 4, "data", "four"),
                record.copy("id", 10, "data", "ten"),
            )
        )
        table.newAppend() // #6
            .appendFile(file6)
            .commit()
    }

    @Test
    fun testCompactWithRemovedFiles() {
        setupTableWithRemovedFiles()
        val compaction = CompactSnapshots(table, 1, 5)
        compaction.execute()

        Assertions.assertEquals(8, table.snapshots().count())
        Assertions.assertEquals(3, table.currentAncestors().count())
        assertNewSnapshot(
            7,
            listOf(
                record.copy("id", 5, "data", "fifth"),
                record.copy("id", 7, "data", "seventh"),
            ),
            listOf(
                eqDelRecord.copy("id", 1),
                eqDelRecord.copy("id", 4),
                eqDelRecord.copy("id", 6),
            ),
            removedDataFiles = listOf(table.location() + "/data/data_a.parquet")
        )
        assertDataConsistent(6, 8)
    }

    private fun assertNewSnapshot(
        snapshotId: Long,
        data: Iterable<Record>,
        eqDeletes: Iterable<Record> = emptyList(),
        posDeletes: Iterable<Record> = emptyList(),
        removedDataFiles: Iterable<String> = emptyList(),
    ) {
        val io = table.io()
        val snapshot = table.snapshot(snapshotId)

        val dataFiles = snapshot.addedDataFiles(io)
        val aData = GenericReader(io, schema).openFile(dataFiles)
        Assertions.assertEquals(
            data.associateBy { it.getField("id") as Int },
            aData.associateBy { it.getField("id") as Int },
        )

        val deleteFiles = snapshot.addedDeleteFiles(io)
        val aEqDeletes = GenericReader(io, eqDelSchema)
            .openFile(deleteFiles.filter { it.content() == EQUALITY_DELETES })
        Assertions.assertEquals(eqDeletes.toSet(), aEqDeletes.toSet())

        val aPosDeletes = GenericReader(io, schema)
            .openFile(deleteFiles.filter { it.content() == POSITION_DELETES })
        Assertions.assertEquals(posDeletes.toSet(), aPosDeletes.toSet())

        val aRemovedDataFiles = snapshot.removedDataFiles(io).map { it.path().toString() }
        Assertions.assertEquals(removedDataFiles.toSet(), aRemovedDataFiles.toSet())
    }

    private fun assertDataConsistent(oldSnapshotId: Long, newSnapshotId: Long) {
        val oldData = readTable(oldSnapshotId)
        val newData = readTable(newSnapshotId)

        Assertions.assertEquals(oldData.size, newData.size)

        val oldIds = oldData.associateBy { it.getField("id") as Int }
        val newIds = oldData.associateBy { it.getField("id") as Int }
        Assertions.assertEquals(oldIds, newIds)
    }

    private fun readTable(snapshotId: Long): List<Record> {
        val io = table.io()
        val snapshot = table.snapshot(snapshotId)
        val fileScanTasks = ManifestGroup(io, snapshot.dataManifests(io), snapshot.deleteManifests(io))
            .specsById(table.specs())
            .ignoreDeleted()
            .planFiles()

        val reader = GenericReader(io, schema)
        return reader.openTask(fileScanTasks).toList()
    }

    private inline fun <reified T : Throwable> assertThrows(executable: Executable): T {
        return Assertions.assertThrows(T::class.java, executable)
    }

    private inline fun <reified T : Throwable> assertThrows(message: String, executable: Executable): T {
        return Assertions.assertThrows(T::class.java, executable, message)
    }
}
