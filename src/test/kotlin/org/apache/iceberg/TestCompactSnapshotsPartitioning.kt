package org.apache.iceberg

import dev.alluvial.sink.iceberg.io.GenericReader
import io.micrometer.core.instrument.Metrics
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.FileHelpers.writeDataFile
import org.apache.iceberg.data.FileHelpers.writeDeleteFile
import org.apache.iceberg.data.GenericAppenderFactory
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.data.Record
import org.apache.iceberg.encryption.EncryptedFiles
import org.apache.iceberg.encryption.EncryptionKeyMetadata
import org.apache.iceberg.io.FileAppenderFactory
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.util.Pair
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.io.IOException

open class TestCompactSnapshotsPartitioning {
    protected val schema = Schema(
        NestedField.required(1, "id", Types.IntegerType.get()),
        NestedField.required(2, "data", Types.StringType.get()),
        NestedField.required(3, "part", Types.StringType.get())
    )
    protected val eqDelSchema: Schema = schema.select("id")
    protected val record: Record = GenericRecord.create(schema)
    protected val eqDelRecord: Record = GenericRecord.create(eqDelSchema)

    @TempDir
    protected lateinit var tableDir: File
    protected lateinit var table: Table
    protected lateinit var metrics: CompactSnapshots.Metrics

    @BeforeEach
    open fun setupTable() {
        tableDir.delete()
        table = TestTables.create(tableDir, "test", schema, PartitionSpec.unpartitioned(), 2)
        table.updateSpec()
            .addField("part")
            .commit()
        table.updateSchema()
            .setIdentifierFields("id")
            .commit()
        metrics = CompactSnapshots.Metrics(Metrics.globalRegistry, TableIdentifier.of("test"))
    }

    @AfterEach
    open fun cleanupTables() {
        TestTables.clearTables()
    }

    private fun setupPartitionedTableWithGlobalDeletes() {
        val io = table.io()
        val partitionData = PartitionData(
            Types.StructType.of(
                NestedField.required(3, "part", Types.StringType.get())
            )
        )
        val snapshot1Data = writeDataFile(
            table, io.newOutputFile(table.location() + "/data/part=part_a/sn1_data.parquet"),
            partitionData.copy().also { it.set(0, "part_a") },
            listOf(
                record.copy("id", 1, "data", "first", "part", "part_a"),
                record.copy("id", 2, "data", "second", "part", "part_a"),
                record.copy("id", 3, "data", "third", "part", "part_a"),
            )
        )
        table.newAppend()
            .appendFile(snapshot1Data)
            .commit()

        val snapshot2Data1 = writeDataFile(
            table, io.newOutputFile(table.location() + "/data/part=part_a/sn2_data1.parquet"),
            partitionData.copy().also { it.set(0, "part_a") },
            listOf(
                record.copy("id", 4, "data", "fourth", "part", "part_a"),
                record.copy("id", 5, "data", "fifth", "part", "part_a"),
            )
        )
        val snapshot2Data2 = writeDataFile(
            table, io.newOutputFile(table.location() + "/data/part=part_b/sn2_data2.parquet"),
            partitionData.copy().also { it.set(0, "part_b") },
            listOf(
                record.copy("id", 6, "data", "sixth", "part", "part_b"),
                record.copy("id", 7, "data", "seventh", "part", "part_b"),
            )
        )
        val snapshot2GlobalDeletes = writeGlobalDeleteFile(
            table, io.newOutputFile(table.location() + "/data/sn2_gdel.parquet"),
            listOf(eqDelRecord.copy("id", 2)), eqDelSchema
        )
        val snapshot2EqualityDeletes = writeDeleteFile(
            table, io.newOutputFile(table.location() + "/data/part=part_a/sn2_edel.parquet"),
            partitionData.copy().also { it.set(0, "part_a") },
            listOf(eqDelRecord.copy("id", 3)), eqDelSchema
        )
        table.newRowDelta()
            .addRows(snapshot2Data1)
            .addRows(snapshot2Data2)
            .addDeletes(snapshot2GlobalDeletes)
            .addDeletes(snapshot2EqualityDeletes)
            .validateDeletedFiles()
            .commit()

        val snapshot3Data = writeDataFile(
            table, io.newOutputFile(table.location() + "/data/part=part_c/sn3_data.parquet"),
            partitionData.copy().also { it.set(0, "part_c") },
            listOf(
                record.copy("id", 8, "data", "eighth", "part", "part_c"),
                record.copy("id", 9, "data", "ninth", "part", "part_c"),
            )
        )
        val snapshot3PositionalDeletes = writeDeleteFile(
            table, io.newOutputFile(table.location() + "/data/part=part_c/sn3_pdel.parquet"),
            partitionData.copy().also { it.set(0, "part_c") },
            listOf(
                Pair.of(snapshot3Data.path(), 0L)
            )
        )
        table.newRowDelta()
            .addRows(snapshot3Data)
            .addDeletes(snapshot3PositionalDeletes.first())
            .validateDataFilesExist(snapshot3PositionalDeletes.second())
            .commit()

        val snapshot4GlobalDeletes = writeGlobalDeleteFile(
            table, io.newOutputFile(table.location() + "/data/sn4_gdel.parquet"),
            listOf(eqDelRecord.copy("id", 9)), eqDelSchema
        )
        table.newRowDelta().addDeletes(snapshot4GlobalDeletes).commit()
    }

    @Test
    fun testCompactWithGlobalDeletes() {
        setupPartitionedTableWithGlobalDeletes()

        val currentSnapshotId = table.currentSnapshotId()!!
        val expectedCompactionId = currentSnapshotId + 1
        val compaction = CompactSnapshots(table, metrics, 1, currentSnapshotId)
        compaction.execute()

        Assertions.assertEquals(2, table.currentAncestorIds().count())
        Assertions.assertEquals(expectedCompactionId, table.currentSnapshot().sequenceNumber())

        val io = table.io()
        val compactionSnapshot = table.snapshot(expectedCompactionId)

        Assertions.assertEquals(1, compactionSnapshot.addedDeleteFiles(io).count { it.isGlobal() })

        // Assert data
        val expectedData = listOf(
            record.copy("id", 1, "data", "first", "part", "part_a"),
            // #2 is deleted
            // #3 is deleted
            record.copy("id", 4, "data", "fourth", "part", "part_a"),
            record.copy("id", 5, "data", "fifth", "part", "part_a"),
            record.copy("id", 6, "data", "sixth", "part", "part_b"),
            record.copy("id", 7, "data", "seventh", "part", "part_b"),
            // #8 is deleted
            // #9 is deleted
        )

        val actualData = IcebergGenerics.read(this.table).build()
        Assertions.assertEquals(
            expectedData.associateBy { it.getField("id") as Int },
            actualData.associateBy { it.getField("id") as Int },
        )

        val deleteFiles = compactionSnapshot.addedDeleteFiles(io)
        // Assert equality deletes
        Assertions.assertEquals(
            setOf(
                eqDelRecord.copy("id", 2),
                eqDelRecord.copy("id", 3),
                eqDelRecord.copy("id", 9)
            ),
            GenericReader(io, eqDelSchema)
                .openFile(deleteFiles.filter { it.content() == FileContent.EQUALITY_DELETES })
                .toSet()
        )
    }

    @Throws(IOException::class)
    open fun writeGlobalDeleteFile(
        table: Table,
        out: OutputFile?,
        deletes: List<Record>?,
        deleteRowSchema: Schema
    ): DeleteFile? {
        val format = FileFormat.PARQUET
        val equalityFieldIds =
            deleteRowSchema.columns().stream().mapToInt(NestedField::fieldId).toArray()
        val factory: FileAppenderFactory<Record> =
            GenericAppenderFactory(
                table.schema(),
                PartitionSpec.unpartitioned(),
                equalityFieldIds,
                deleteRowSchema,
                null
            )
        val encryptedFile = EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY)
        val writer = factory.newEqDeleteWriter(encryptedFile, format, null)

        writer.use {
            it.write(deletes)
        }

        return writer.toDeleteFile()
    }

    private fun DeleteFile.isGlobal() = this.specId() == PartitionSpec.unpartitioned().specId()
}
