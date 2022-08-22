package org.apache.iceberg

import dev.alluvial.sink.iceberg.io.GenericReader
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.Record
import org.apache.iceberg.types.Types
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.io.TempDir
import java.io.File

@Suppress("MemberVisibilityCanBePrivate")
open class TestCompactSnapshotsBase {
    private val schema = Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.StringType.get())
    )
    protected val eqDelSchema: Schema = schema.select("id")
    protected val posDelSchema: Schema = POS_DELETE_SCHEMA
    protected val spec: PartitionSpec = PartitionSpec.builderFor(schema).build()
    protected val record: Record = GenericRecord.create(schema)
    protected val eqDelRecord: Record = GenericRecord.create(eqDelSchema)
    protected val posDelRecord: Record = GenericRecord.create(posDelSchema)

    @TempDir
    protected lateinit var tableDir: File
    protected lateinit var table: TestTables.TestTable

    @BeforeEach
    fun setupTable() {
        tableDir.delete()
        table = TestTables.create(tableDir, "test", schema, spec, 2)
    }

    @AfterEach
    fun cleanupTables() {
        TestTables.clearTables()
    }


    protected fun assertNewSnapshot(
        snapshotId: Long,
        data: Iterable<Record>,
        eqDeletes: Iterable<Record> = emptyList(),
        posDeletes: Iterable<Record> = emptyList(),
        removedDataFiles: Iterable<String> = emptyList(),
    ) {
        val io = table.io()
        val snapshot = table.snapshot(snapshotId)
        val schema = table.schemas()[snapshot.schemaId()]!!

        val dataFiles = snapshot.addedDataFiles(io)
        val aData = GenericReader(io, schema).openFile(dataFiles)
        Assertions.assertEquals(
            data.associateBy { it.getField("id") as Int },
            aData.associateBy { it.getField("id") as Int },
        )

        val deleteFiles = snapshot.addedDeleteFiles(io)
        val aEqDeletes = GenericReader(io, eqDelSchema)
            .openFile(deleteFiles.filter { it.content() == FileContent.EQUALITY_DELETES })
        Assertions.assertEquals(eqDeletes.toSet(), aEqDeletes.toSet())

        val aPosDeletes = GenericReader(io, schema)
            .openFile(deleteFiles.filter { it.content() == FileContent.POSITION_DELETES })
        Assertions.assertEquals(posDeletes.toSet(), aPosDeletes.toSet())

        val aRemovedDataFiles = snapshot.removedDataFiles(io).map { it.path().toString() }
        Assertions.assertEquals(removedDataFiles.toSet(), aRemovedDataFiles.toSet())
    }

    protected fun assertDataConsistent(oldSnapshotId: Long, newSnapshotId: Long) {
        val oldData = readTable(oldSnapshotId)
        val newData = readTable(newSnapshotId)

        Assertions.assertEquals(oldData.size, newData.size)

        val oldIds = oldData.associateBy { it.getField("id") as Int }
        val newIds = oldData.associateBy { it.getField("id") as Int }
        Assertions.assertEquals(oldIds, newIds)
    }

    protected fun readTable(snapshotId: Long): List<Record> {
        val io = table.io()
        val snapshot = table.snapshot(snapshotId)
        val schema = table.schemas()[snapshot.schemaId()]!!
        val fileScanTasks = ManifestGroup(io, snapshot.dataManifests(io), snapshot.deleteManifests(io))
            .specsById(table.specs())
            .ignoreDeleted()
            .planFiles()

        val reader = GenericReader(io, schema)
        return reader.openTask(fileScanTasks).toList()
    }

    protected inline fun <reified T : Throwable> assertThrows(executable: Executable): T {
        return Assertions.assertThrows(T::class.java, executable)
    }

    protected inline fun <reified T : Throwable> assertThrows(message: String, executable: Executable): T {
        return Assertions.assertThrows(T::class.java, executable, message)
    }
}
