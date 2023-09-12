package org.apache.iceberg

import org.apache.iceberg.data.FileHelpers.writeDataFile
import org.apache.iceberg.data.FileHelpers.writeDeleteFile
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.types.Types.StringType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestCompactSnapshotsWithSchemaMigration : TestCompactSnapshotsBase() {
    private fun setupTableWithMigration() {
        val io = table.io()
        val file1 = writeDataFile(
            table, io.newOutputFile(table.location() + "/data/data_a.parquet"), listOf(
                record.copy("id", 1, "data", "first"),
                record.copy("id", 2, "data", "second"),
                record.copy("id", 3, "data", "third"),
            )
        )
        table.newAppend() // #1
            .appendFile(file1)
            .commit()

        val file2 = writeDataFile(
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

        table.updateSchema()
            .renameColumn("data", "payload")
            .addColumn("extra", StringType.get())
            .commit()

        val newRecord = GenericRecord.create(table.schema())
        val file3 = writeDataFile(
            table, io.newOutputFile(table.location() + "/data/data_c.parquet"), listOf(
                newRecord.copy("id", 10, "payload", "ten", "extra", "a"),
                newRecord.copy("id", 11, "payload", "eleven", "extra", "b"),
                newRecord.copy("id", 12, "payload", "twelve", "extra", "c"),
            )
        )
        table.newAppend() // #3
            .appendFile(file3)
            .commit()

        val file4 = writeDeleteFile(
            table, io.newOutputFile(table.location() + "/data/eq_deletes_c.parquet"), listOf(
                eqDelRecord.copy("id", 12),
                eqDelRecord.copy("id", 6),
                eqDelRecord.copy("id", 3),
            ), eqDelSchema
        )
        table.newRowDelta() // #4
            .addDeletes(file4)
            .commit()
    }

    @Test
    fun testCompactWithSimpleMigration() {
        setupTableWithMigration()
        val oldHeadId = table.currentSnapshotId()!!
        val highSnapshotId = 3L

        CompactSnapshots(1, highSnapshotId, table, tracer, meter)
            .execute()

        val newHeadId = table.currentSnapshotId()!!
        val compactedSnapshotId = 5L

        Assertions.assertEquals(6, table.snapshots().count())
        Assertions.assertEquals(3, table.currentAncestors().count())
        Assertions.assertEquals(
            table.snapshot(highSnapshotId).schemaId(),
            table.snapshot(compactedSnapshotId).schemaId()
        )
        assertDataConsistent(oldHeadId, newHeadId)
    }

    @Test
    fun testCompactBeforeSchemaChanged() {
        setupTableWithMigration()
        val oldHeadId = table.currentSnapshotId()!!
        val highSnapshotId = 2L

        CompactSnapshots(null, highSnapshotId, table, tracer, meter)
            .execute()

        val newHeadId = table.currentSnapshotId()!!
        val compactedSnapshotId = 5L

        Assertions.assertEquals(7, table.snapshots().count())
        Assertions.assertEquals(3, table.currentAncestors().count())
        Assertions.assertEquals(
            table.snapshot(highSnapshotId).schemaId(),
            table.snapshot(compactedSnapshotId).schemaId()
        )
        assertDataConsistent(oldHeadId, newHeadId)
    }

    @Test
    fun testCompactAfterSchemaChanged() {
        setupTableWithMigration()
        val oldHeadId = table.currentSnapshotId()!!
        CompactSnapshots(2, oldHeadId, table, tracer, meter)
            .execute()
        val newHeadId = table.currentSnapshotId()!!

        Assertions.assertEquals(5, table.snapshots().count())
        Assertions.assertEquals(3, table.currentAncestors().count())
        Assertions.assertEquals(
            table.snapshot(newHeadId).schemaId(),
            table.snapshot(oldHeadId).schemaId(),
        )
        Assertions.assertEquals(
            table.snapshot(newHeadId).schemaId(),
            table.schema().schemaId()
        )
        assertDataConsistent(oldHeadId, newHeadId)
    }
}
