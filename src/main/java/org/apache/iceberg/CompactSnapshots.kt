package org.apache.iceberg

import dev.alluvial.sink.iceberg.concat
import dev.alluvial.sink.iceberg.filter
import dev.alluvial.sink.iceberg.io.GenericFileWriterFactory
import dev.alluvial.sink.iceberg.io.GenericReader
import dev.alluvial.sink.iceberg.io.PartitioningWriter
import dev.alluvial.sink.iceberg.io.PartitioningWriterFactory
import dev.alluvial.sink.iceberg.transform
import dev.alluvial.sink.iceberg.type.IcebergSchema
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.LongTaskTimer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.apache.iceberg.FileContent.POSITION_DELETES
import org.apache.iceberg.TableProperties.*
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.Record
import org.apache.iceberg.deletes.Deletes
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.DataWriteResult
import org.apache.iceberg.io.DeleteWriteResult
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.io.WriteResult
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.util.PropertyUtil
import org.apache.iceberg.util.StructLikeMap
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.function.Supplier

/**
 * NOTE: test the following use-cases
 * * change `Schema` in the middle of CompactionGroup
 * * change `PartitionSpec` in the middle of CompactionGroup => illegal
 * * change `identityIds` in the middle of CompactionGroup
 * * mix `DataOperations` in CompactionGroup
 * * mix data/deletes file in different sequenceId in the snapshot
 * @see org.apache.iceberg.ManifestGroup
 * @see org.apache.iceberg.data.DeleteFilter.filter
 */
class CompactSnapshots(
    private val table: Table,
    private val metrics: Metrics,
    lowSnapshotId: Long?,
    highSnapshotId: Long,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(CompactSnapshots::class.java)
        private val PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)
        private val TASK_FORMATTER = DateTimeFormatter.ofPattern("HHmmss").withZone(ZoneOffset.UTC)
    }

    private val io = table.io()
    private val specsById = table.specs()
    private val schemasById = table.schemas()

    private val lowSnapshot = lowSnapshotId?.let(table::snapshot)
    private val highSnapshot = table.snapshot(highSnapshotId)
    private val schema = highSnapshot.schema()
    private val spec = highSnapshot.spec()

    private val fileWriterFactoryBuilder: GenericFileWriterFactory.Builder
    private val partitioningWriterFactoryBuilder: PartitioningWriterFactory.Builder<Record>
    private val resultBuilder = WriteResult.builder()

    init {
        val ancestorIds = table.currentAncestorIds()
        assert((lowSnapshotId == null || lowSnapshotId in ancestorIds) && highSnapshotId in ancestorIds) {
            "lowSnapshotId & highSnapshotId MUST be in current timeline"
        }
        assert(lowSnapshot == null || lowSnapshot.sequenceNumber() < highSnapshot.sequenceNumber()) {
            "sequenceNumber of lowSnapshot MUST be < highSnapshot"
        }

        fileWriterFactoryBuilder = GenericFileWriterFactory.builder(table).apply {
            dataSchema = schema
            equalityFieldIds = schema.identifierFieldIds().toIntArray()
        }

        val targetFileSizeInBytes = PropertyUtil.propertyAsLong(
            table.properties(),
            WRITE_TARGET_FILE_SIZE_BYTES,
            WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        )
        val now = Instant.now()
        val partitionId = PARTITION_FORMATTER.format(now).toInt()
        val taskId = TASK_FORMATTER.format(now).toLong()
        val outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
            .build()
        partitioningWriterFactoryBuilder = PartitioningWriterFactory.builder<Record>()
            .fileFactory(outputFileFactory)
            .io(io)
            .targetFileSizeInBytes(targetFileSizeInBytes)
    }

    fun execute() {
        if (lowSnapshot != null) metrics.recordCheckPositionDelete(::checkPositionDelete)
        metrics.recordRewriteData(::rewriteData)
        if (lowSnapshot != null) metrics.recordRewriteEqualityDelete(::rewriteEqualityDelete)

        apply()
    }

    /**
     * @see org.apache.iceberg.AlluvialSquashOperation.validatePosDeletesReferenceToDataFileInRange
     */
    private fun checkPositionDelete() {
        val ancestors = table.ancestorsOf(highSnapshot)
            .filterAfter(lowSnapshot)
            .reversed()

        val dataFiles = mutableSetOf<CharSequence>()
        ancestors.forEach { snapshot ->
            snapshot.addedDataFiles(io)
                .map(DataFile::path)
                .let(dataFiles::addAll)
            val posDelFiles = snapshot.addedDeleteFiles(io)
                .filter { it.content() == POSITION_DELETES }

            val filter = Expressions.notIn(MetadataColumns.DELETE_FILE_PATH.name(), dataFiles)
            val records = GenericReader(io, POS_DELETE_SCHEMA)
                .openFile(posDelFiles, filter)

            records.useResource {
                if (it.any()) {
                    throw ValidationException(
                        "Snapshot %s contains POSITION_DELETES file reference to out of CompactionGroup",
                        snapshot.snapshotId()
                    )
                }
            }
        }
    }

    private fun rewriteData() {
        val fileGroups = planFileGroups()
        val writer = newDataWriter()

        writer.useResource {
            fileGroups.forEach { (partition, files) ->
                rewriteDataForPartition(writer, partition, files)
            }
        }
        resultBuilder.addDataFiles(writer.result().dataFiles())
    }

    /**
     * @see org.apache.iceberg.spark.actions.RewriteDataFilesSparkAction#planFileGroups
     */
    private fun planFileGroups(): Map<StructLike, List<FileScanTask>> {
        val fileScanTasks = ManifestGroup(io, highSnapshot.dataManifests(io), highSnapshot.deleteManifests(io))
            .specsById(specsById)
            .filterEntryAfter(lowSnapshot)
            .ignoreDeleted()
            .planFiles()

        val partitionType = spec.partitionType()
        val filesByPartition = StructLikeMap.create<MutableList<FileScanTask>>(partitionType)

        fileScanTasks.useResource { fst ->
            fst.groupByTo(filesByPartition) {
                if (it.file().specId() != spec.specId()) {
                    throw IllegalStateException("Detect specId changed in middle of CompactionGroup")
                }
                it.file().partition()
            }
        }

        return filesByPartition
    }

    private fun rewriteDataForPartition(
        writer: PartitioningWriter<Record, DataWriteResult>,
        partition: StructLike,
        files: List<FileScanTask>
    ) {
        val reader = GenericReader(io, schema)
        val iterable = reader.openTask(files)

        iterable.useResource {
            for (rec in it) {
                writer.write(rec, spec, partition)
            }
        }
    }

    private fun rewriteEqualityDelete() {
        val fileGroups = planEqualityDeleteFiles()

        fileGroups.forEach { (ids, partitionGroup) ->
            val deleteSchema = TypeUtil.select(schema, ids)
            val writer = newEqualityDeleteWriter(ids, deleteSchema)
            writer.useResource {
                partitionGroup.forEach { (partition, files) ->
                    rewriteEqualityDeleteForPartition(writer, deleteSchema, partition, files)
                }
            }
            resultBuilder.addDeleteFiles(writer.result().deleteFiles())
        }
    }

    /**
     * @see org.apache.iceberg.DeleteFileIndex.Builder.build()
     */
    private fun planEqualityDeleteFiles(): Map<Set<Int>, Map<StructLike, List<DeleteFile>>> {
        val deleteEntries = highSnapshot.deleteManifests(io)
            .filterAfter(lowSnapshot)
            .filter { it.hasAddedFiles() || it.hasExistingFiles() } // ignoreDeleted ManifestFile
            .transform {
                ManifestFiles.readDeleteManifest(it, io, specsById)
                    .liveEntries() // ignoreDeleted ManifestFile
            }
            .concat()
            .filterAfter(lowSnapshot) // ignoreDeleted ManifestFile
            .filter { it.file().content() == FileContent.EQUALITY_DELETES }

        val partitionType = spec.partitionType()
        val fileGroups = mutableMapOf<Set<Int>, StructLikeMap<MutableList<DeleteFile>>>()

        deleteEntries
            .transform { it.copy().file() } // ManifestReader.open using reuseContainers
            .forEach {
                if (it.specId() != spec.specId()) {
                    throw IllegalStateException("Detect specId changed in middle of CompactionGroup")
                }
                val filesByPartition = fileGroups.computeIfAbsent(it.equalityFieldIds().toSet()) {
                    StructLikeMap.create(partitionType)
                }
                val fileGroup = filesByPartition.computeIfAbsent(it.partition()) {
                    arrayListOf()
                }
                fileGroup.add(it)
            }

        return fileGroups
    }

    /**
     * @see org.apache.iceberg.data.DeleteFilter.applyEqDeletes()
     */
    private fun rewriteEqualityDeleteForPartition(
        writer: PartitioningWriter<Record, DeleteWriteResult>,
        deleteSchema: IcebergSchema,
        partition: StructLike,
        files: List<DeleteFile>
    ) {
        // reuseContainers=false to give each record its own object
        // otherwise transform(Record::copy) is need because delete records will be held in a set
        val reader = GenericReader(io, deleteSchema, reuseContainers = false)
        val iterable = reader.openFile(files)

        @Suppress("UNCHECKED_CAST")
        val deleteSet = Deletes.toEqualitySet(
            iterable as CloseableIterable<StructLike>,
            deleteSchema.asStruct()
        ) as Set<Record>


        for (rec in deleteSet) {
            writer.write(rec, spec, partition)
        }
    }

    /**
     * @see org.apache.iceberg.RemoveSnapshots.internalApply
     */
    private fun apply() {
        val txn = newTransaction()
        val squash = txn.squash()
        squash.squash(lowSnapshot?.snapshotId(), highSnapshot.snapshotId())
            .validateDeleteFilesInRange(false) // already validated

        val result = resultBuilder.build()
        result.dataFiles().forEach(squash::add)
        result.deleteFiles().forEach(squash::add)
        metrics.measureWriteResult(result)

        metrics.recordCommit {
            squash.commit()
            txn.commitTransaction()
        }
    }

    private fun newTransaction(): AlluvialTransaction {
        return AlluvialTransaction.of(table)
    }

    private fun newDataWriter(): PartitioningWriter<Record, DataWriteResult> {
        val partitioningWriterFactory = partitioningWriterFactoryBuilder
            .writerFactory(fileWriterFactoryBuilder.build())
            .buildForClusteredPartition()

        return partitioningWriterFactory.newDataWriter()
    }

    private fun newEqualityDeleteWriter(
        ids: Set<Int>,
        deleteSchema: IcebergSchema
    ): PartitioningWriter<Record, DeleteWriteResult> {
        fileWriterFactoryBuilder.apply {
            equalityFieldIds = ids.toIntArray()
            equalityDeleteRowSchema = deleteSchema
        }
        val partitioningWriterFactory = partitioningWriterFactoryBuilder
            .writerFactory(fileWriterFactoryBuilder.build())
            .buildForClusteredPartition()

        return partitioningWriterFactory.newEqualityDeleteWriter()
    }

    private inline fun <T : Closeable, R> T.useResource(block: (T) -> R): R {
        try {
            return block(this)
        } finally {
            try {
                this.close()
            } catch (ioe: IOException) {
                logger.error("Cannot properly close this resource", ioe)
            }
        }
    }

    private fun Snapshot.spec(): PartitionSpec {
        val aManifest = this.allManifests(io)
            .maxWith(Comparator.comparingLong(ManifestFile::sequenceNumber))
        return specsById[aManifest.partitionSpecId()]!!
    }

    private fun Snapshot.schema(): Schema {
        return schemasById[this.schemaId()]!!
    }

    class Metrics(
        private val registry: MeterRegistry,
        tableId: TableIdentifier,
    ) : Closeable {
        private val tags: Tags = Tags.of("table", tableId.toString())

        private val successCount = Counter.builder("alluvial.compact")
            .tags(tags).tag("status", "success")
            .description("Total number of compaction run")
            .register(registry)

        private val failureCount = Counter.builder("alluvial.compact")
            .tags(tags).tag("status", "failure")
            .description("Total number of compaction run")
            .register(registry)

        ///////////// Duration /////////////
        private val checkPositionDeleteDuration =
            LongTaskTimer.builder("alluvial.compact")
                .tags(tags).tag("step", "check_position_deletes")
                .description("CompactSnapshots check POSITION_DELETES duration")
                .register(registry)

        private val rewriteDataDuration = LongTaskTimer.builder("alluvial.compact")
            .tags(tags).tag("step", "rewrite_data")
            .description("CompactSnapshots rewrite DATA duration")
            .register(registry)

        private val rewriteEqualityDeleteDuration =
            LongTaskTimer.builder("alluvial.compact")
                .tags(tags).tag("step", "rewrite_equality_deletes")
                .description("CompactSnapshots rewrite EQUALITY_DELETES duration")
                .register(registry)

        private val commitDuration = LongTaskTimer.builder("alluvial.compact")
            .tags(tags).tag("step", "commit")
            .description("CompactSnapshots commit duration")
            .register(registry)

        ///////////// Records per file /////////////
        private val recordCountSummaries = buildMap {
            FileContent.values().forEach {
                val name = it.name.lowercase()
                val summary = DistributionSummary.builder("alluvial.compact.records")
                    .tags(tags).tag("content", name)
                    .description("Number of records per file")
                    .maximumExpectedValue(500_000.0)
                    .serviceLevelObjectives(1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 500_000.0)
                    .register(registry)
                put(it, summary)
            }
        }

        ///////////// File size in bytes /////////////
        private val fileSizeSummaries = buildMap {
            FileContent.values().forEach {
                val name = it.name.lowercase()
                val summary = DistributionSummary.builder("alluvial.compact.file_size")
                    .tags(tags).tag("content", name)
                    .description("File size in bytes")
                    .baseUnit("bytes")
                    .maximumExpectedValue(512.0 * 1024 * 1024) // 512MiB
                    .serviceLevelObjectives(
                        1.0 * 1024, // 1KiB
                        10.0 * 1024, // 10KiB
                        100.0 * 1024, // 100KiB
                        1.0 * 1024 * 1024, // 1MiB
                        10.0 * 1024 * 1024, // 10MiB
                        32.0 * 1024 * 1024, // 32MiB
                        64.0 * 1024 * 1024, // 64MiB
                        128.0 * 1024 * 1024, // 128MiB
                        256.0 * 1024 * 1024, // 256MiB
                        512.0 * 1024 * 1024, // 512MiB
                    )
                    .register(registry)
                put(it, summary)
            }
        }
        private val meters = listOf(
            successCount, failureCount, commitDuration,
            checkPositionDeleteDuration, rewriteDataDuration, rewriteEqualityDeleteDuration,
        ) + recordCountSummaries.values + fileSizeSummaries.values

        fun increment(success: Boolean) {
            val meter = if (success) successCount else failureCount
            meter.increment()
        }

        fun <T> recordCheckPositionDelete(block: Supplier<T>): T {
            return checkPositionDeleteDuration.record(block)
        }

        fun <T> recordRewriteData(block: Supplier<T>): T {
            return rewriteDataDuration.record(block)
        }

        fun <T> recordRewriteEqualityDelete(block: Supplier<T>): T {
            return rewriteEqualityDeleteDuration.record(block)
        }

        fun <T> recordCommit(block: Supplier<T>): T {
            return commitDuration.record(block)
        }

        fun measureWriteResult(result: WriteResult) {
            result.dataFiles().forEach(::trackRecordCount)
            result.deleteFiles().forEach(::trackRecordCount)
            result.dataFiles().forEach(::trackFileSize)
            result.deleteFiles().forEach(::trackFileSize)
        }

        private fun trackRecordCount(file: ContentFile<*>) {
            val summary = recordCountSummaries[file.content()] ?: return
            summary.record(file.recordCount().toDouble())
        }

        private fun trackFileSize(file: ContentFile<*>) {
            val summary = fileSizeSummaries[file.content()] ?: return
            summary.record(file.fileSizeInBytes().toDouble())
        }

        override fun close() {
            meters.forEach {
                registry.remove(it)
                it.close()
            }
        }
    }
}
