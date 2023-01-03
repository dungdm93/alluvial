package org.apache.iceberg

import dev.alluvial.sink.iceberg.concat
import dev.alluvial.sink.iceberg.transform
import io.micrometer.core.instrument.Metrics
import io.mockk.every
import io.mockk.spyk
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP
import org.apache.iceberg.TableProperties.FORMAT_VERSION
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.FileHelpers
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.util.Pair
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

class TestCompactSnapshotsConcurrent : TestCompactSnapshotsBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(AlluvialTransaction::class.java)
    }

    private val tableId = TableIdentifier.of("default", "test")
    private lateinit var catalog: Catalog

    @BeforeEach
    override fun setupTable() {
        tableDir.delete()

        catalog = CatalogUtil.loadCatalog(
            HadoopCatalog::class.qualifiedName, "iceberg",
            mapOf("warehouse" to tableDir.path),
            Configuration()
        )
        table = catalog.buildTable(tableId, schema)
            .withProperty(FORMAT_VERSION, "2")
            .create()
        table.updateSchema()
            .setIdentifierFields("id")
            .commit()

        metrics = CompactSnapshots.Metrics(Metrics.globalRegistry, tableId)
    }

    @AfterEach
    override fun cleanupTables() {
        tableDir.delete()
    }

    private fun setupTableForNormalCompact() {
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
        logger.info("===== FINISH SETUP TABLE SNAPSHOTS =====")
    }

    private fun simulateConcurrentWrite(conSnapshot: AtomicReference<Snapshot>) {
        // Simulate concurrent write by other process.
        // Don't use table variable
        val newTable = catalog.loadTable(tableId)
        val io = newTable.io()
        val file1 = FileHelpers.writeDataFile(
            newTable, io.newOutputFile(table.location() + "/data/data_z.parquet"), listOf(
                record.copy("id", 1000, "data", "oo")
            )
        )
        logger.warn("=====> Simulate concurrent write")
        newTable.newFastAppend()
            .appendFile(file1)
            .commit()
        conSnapshot.set(newTable.currentSnapshot())
        logger.warn("=====> Simulate concurrent write DONE!!!")
    }

    private fun Iterable<Snapshot>.ssWithSeqNumber(seqNum: Long): Snapshot {
        return this.first { it.sequenceNumber() == seqNum }
    }

    private fun Snapshot.dataEntries(): Iterable<ManifestEntry<DataFile>> {
        val io = table.io()
        val specsById = table.specs()
        return this.dataManifests(io)
            .transform {
                ManifestFiles.read(it, io, specsById)
                    .liveEntries() // ignoreDeleted ManifestFile
            }
            .concat()
    }

    private fun Snapshot.deleteEntries(): Iterable<ManifestEntry<DeleteFile>> {
        val io = table.io()
        val specsById = table.specs()
        return this.deleteManifests(io)
            .transform {
                ManifestFiles.readDeleteManifest(it, io, specsById)
                    .liveEntries() // ignoreDeleted ManifestFile
            }
            .concat()
    }

    private fun assertManifestEntry(snap: Snapshot, type: String) {
        snap.dataEntries().forEach {
            val s = table.snapshot(it.snapshotId())
            Assertions.assertEquals(s.sequenceNumber(), it.dataSequenceNumber()) {
                "%s snapshot(#%d, %d) contains DataEntries(%d).sequenceNumber() mismatch: %s".format(
                    type, snap.sequenceNumber(), snap.snapshotId(), it.snapshotId(), it.file().path()
                )
            }
        }

        snap.deleteEntries().forEach {
            val s = table.snapshot(it.snapshotId())
            Assertions.assertEquals(s.sequenceNumber(), it.dataSequenceNumber()) {
                "%s snapshot(#%d, %d) contains DeleteEntries(%d).sequenceNumber() mismatch: %s".format(
                    type, snap.sequenceNumber(), snap.snapshotId(), it.snapshotId(), it.file().path()
                )
            }
        }
    }

    private fun assertCherryPick(cherryPicked: Snapshot, original: Snapshot) {
        Assertions.assertEquals(
            cherryPicked.summary()[SOURCE_SNAPSHOT_ID_PROP], original.snapshotId().toString(), SOURCE_SNAPSHOT_ID_PROP
        )
        cherryPicked.allManifests(table.io())
            .filter { it.snapshotId() == cherryPicked.snapshotId() }
            .forEach {
                Assertions.assertEquals(
                    cherryPicked.sequenceNumber(), it.sequenceNumber(),
                    "ManifestFile.sequenceNumber MUST be equals Snapshot.sequenceNumber()"
                )
            }

        // Assert data consistent
        Assertions.assertEquals(
            original.addedDataFiles(table.io()).mapTo(hashSetOf()) { it.path() },
            cherryPicked.addedDataFiles(table.io()).mapTo(hashSetOf()) { it.path() },
        )
        Assertions.assertEquals(
            original.addedDeleteFiles(table.io()).mapTo(hashSetOf()) { it.path() },
            cherryPicked.addedDeleteFiles(table.io()).mapTo(hashSetOf()) { it.path() },
        )
        Assertions.assertEquals(
            original.removedDataFiles(table.io()).mapTo(hashSetOf()) { it.path() },
            cherryPicked.removedDataFiles(table.io()).mapTo(hashSetOf()) { it.path() },
        )
        Assertions.assertEquals(
            original.removedDeleteFiles(table.io()).mapTo(hashSetOf()) { it.path() },
            cherryPicked.removedDeleteFiles(table.io()).mapTo(hashSetOf()) { it.path() },
        )

        assertManifestEntry(cherryPicked, "cherry-picked")
    }

    private fun assertSquash(squashed: Snapshot, high: Snapshot, low: Snapshot) {
        squashed.allManifests(table.io())
            .filter { it.snapshotId() == squashed.snapshotId() }
            .forEach {
                Assertions.assertEquals(
                    squashed.sequenceNumber(), it.sequenceNumber(),
                    "ManifestFile.sequenceNumber MUST be equals Snapshot.sequenceNumber()"
                )
            }

        assertManifestEntry(squashed, "squashed")
    }

    @Test
    fun testCommitRetry() {
        setupTableForNormalCompact()
        val conSnapshot = AtomicReference<Snapshot>(null)
        val txn = spyk(AlluvialTransaction.of(table)) {
            every { table() } returns spyk(TransactionTable()) {
                every { operations() } returns TransactionTableOperations()
            }
            every { commitTransaction() } answers {
                simulateConcurrentWrite(conSnapshot)
                callOriginal()
            }
        }

        val snapshots = table.currentAncestors().toList()
        val highSnapshot = snapshots.ssWithSeqNumber(3)
        val lowSnapshot = snapshots.ssWithSeqNumber(1)
        val compaction = spyk(CompactSnapshots(table, metrics, lowSnapshot.snapshotId(), highSnapshot.snapshotId())) {
            val com = this
            every { com.invokeNoArgs("newTransaction") } returns txn
        }
        compaction.execute()
        val newSnapshots = table.currentAncestors().toList()
        val squashedSnapshot = newSnapshots[2]

        Assertions.assertEquals(
            conSnapshot.get().sequenceNumber() + 1, squashedSnapshot.sequenceNumber(),
            "Wrong squashedSnapshot.sequenceNumber"
        )
        assertSquash(squashedSnapshot, highSnapshot, lowSnapshot)

        assertCherryPick(newSnapshots[1], snapshots[0])
        assertCherryPick(newSnapshots[0], conSnapshot.get())
    }
}
