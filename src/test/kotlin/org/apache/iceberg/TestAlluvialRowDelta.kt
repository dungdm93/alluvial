package org.apache.iceberg

import org.apache.iceberg.data.FileHelpers
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.types.Types
import org.apache.iceberg.util.Pair
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class TestAlluvialRowDelta {
    private val schema = Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.StringType.get())
    )
    private val eqDelSchema: Schema = schema.select("id")
    private val posDelSchema: Schema = POS_DELETE_SCHEMA
    private val spec: PartitionSpec = PartitionSpec.builderFor(schema).build()
    private val record: Record = GenericRecord.create(schema)
    private val eqDelRecord: Record = GenericRecord.create(eqDelSchema)
    private val posDelRecord: Record = GenericRecord.create(posDelSchema)

    @TempDir
    private lateinit var tableDir: File
    private lateinit var table: TestTables.TestTable

    @BeforeEach
    fun setupTable() {
        tableDir.delete()
        table = TestTables.create(tableDir, "test", schema, spec, 2)

        setupTableSnapshots()
    }

    private fun setupTableSnapshots() {
        val io = table.io()
        val file1 = FileHelpers.writeDataFile(
            table, io.newOutputFile(table.location() + "/data/data_a.parquet"), listOf(
                record.copy("id", 1, "data", "first"),
                record.copy("id", 2, "data", "second"),
                record.copy("id", 3, "data", "third"),
            )
        )
        table.newAppend() // #1
            .appendFile(file1)
            .commit()

        val file2 = FileHelpers.writeDataFile(
            table, io.newOutputFile(table.location() + "/data/data_b.parquet"), listOf(
                record.copy("id", 4, "data", "fourth"),
                record.copy("id", 5, "data", "fifth"),
                record.copy("id", 6, "data", "sixth"),
                record.copy("id", 7, "data", "seventh"),
            )
        )
        table.newAppend() // #2
            .appendFile(file2)
            .commit()

        val file3 = FileHelpers.writeDeleteFile(
            table, io.newOutputFile(table.location() + "/data/eq_deletes_ab.parquet"), listOf(
                eqDelRecord.copy("id", 4),
                eqDelRecord.copy("id", 6),
                eqDelRecord.copy("id", 1),
            ), eqDelSchema
        )
        val file4 = FileHelpers.writeDataFile(
            table, io.newOutputFile(table.location() + "/data/data_c.parquet"), listOf(
                record.copy("id", 10, "data", "ten"),
            )
        )
        val file5 = FileHelpers.writeDeleteFile(
            table, io.newOutputFile(table.location() + "/data/pos_deletes_bc.parquet"), listOf(
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

        val file6 = FileHelpers.writeDataFile(
            table, io.newOutputFile(table.location() + "/data/data_d.parquet"), listOf(
                record.copy("id", 6, "data", "six"),
                record.copy("id", 7, "data", "seven"),
            )
        )
        table.newAppend() // #4
            .appendFile(file6)
            .commit()
    }

    @AfterEach
    fun cleanupTables() {
        TestTables.clearTables()
    }

    private fun changeForRowDelta(rowDelta: RowDelta) {
        val io = table.io()

        val fileData = FileHelpers.writeDataFile(
            table, io.newOutputFile(table.location() + "/data/data_e.parquet"), listOf(
                record.copy("id", 20, "data", "twenty"),
                record.copy("id", 21, "data", "twenty-one"),
                record.copy("id", 22, "data", "twenty-two"),
            )
        )
        val fileEqDel = FileHelpers.writeDeleteFile(
            table, io.newOutputFile(table.location() + "/data/eq_deletes.parquet"), listOf(
                eqDelRecord.copy("id", 3),
                eqDelRecord.copy("id", 7),
                eqDelRecord.copy("id", 21),
            ), eqDelSchema
        )
        val filePosDel = FileHelpers.writeDeleteFile(
            table, io.newOutputFile(table.location() + "/data/pos_deletes.parquet"), listOf(
                Pair.of(fileData.path(), 0L),
            )
        )

        rowDelta
            .addRows(fileData)
            .addDeletes(fileEqDel)
            .addDeletes(filePosDel.first())
            .validateDataFilesExist(filePosDel.second())
    }

    @Test
    fun testAlluvialRowDeltaSuccess() {
        val rowDelta = AlluvialRowDelta.of(table)
            .validateFromHead()
            .validateDeletedFiles()
        changeForRowDelta(rowDelta)

        // before rowDelta committed, HEAD had changed
        table.manageSnapshots()
            .rollbackTo(2)
            .commit()

        rowDelta.commit()

        Assertions.assertEquals(3, table.currentAncestors().count())

        val snapshot = table.currentSnapshot()
        Assertions.assertEquals(5, snapshot.snapshotId())
        Assertions.assertEquals(2, snapshot.parentId())
    }

    @Test
    fun testAlluvialRowDeltaFails() {
        val rowDelta = table.newRowDelta()
            .validateFromSnapshot(table.currentSnapshotId()!!)
            .validateDeletedFiles()
        changeForRowDelta(rowDelta)

        // before rowDelta committed, HEAD had changed
        table.manageSnapshots()
            .rollbackTo(2)
            .commit()

        val e = Assertions.assertThrows(ValidationException::class.java, rowDelta::commit)
        Assertions.assertEquals(
            "Cannot determine history between starting snapshot 4 and the last known ancestor 1",
            e.message
        )
    }
}
