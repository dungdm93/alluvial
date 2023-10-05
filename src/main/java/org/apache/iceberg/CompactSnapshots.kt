package org.apache.iceberg

import dev.alluvial.sink.iceberg.concat
import dev.alluvial.sink.iceberg.filter
import dev.alluvial.sink.iceberg.io.GenericFileWriterFactory
import dev.alluvial.sink.iceberg.io.GenericReader
import dev.alluvial.sink.iceberg.io.PartitioningWriter
import dev.alluvial.sink.iceberg.io.PartitioningWriterFactory
import dev.alluvial.sink.iceberg.transform
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.utils.ICEBERG_PARTITION
import dev.alluvial.utils.ICEBERG_SCHEMA
import dev.alluvial.utils.ICEBERG_TABLE
import dev.alluvial.utils.withSpan
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import org.apache.iceberg.FileContent.POSITION_DELETES
import org.apache.iceberg.TableProperties.*
import org.apache.iceberg.data.Record
import org.apache.iceberg.deletes.Deletes
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.DataWriteResult
import org.apache.iceberg.io.DeleteWriteResult
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.io.WriteResult
import org.apache.iceberg.types.Comparators
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.util.PropertyUtil
import org.apache.iceberg.util.StructLikeMap
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

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
    lowSnapshotId: Long?,
    highSnapshotId: Long,
    private val table: Table,
    private val tracer: Tracer,
    private val meter: Meter,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(CompactSnapshots::class.java)
        private val PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)
        private val TASK_FORMATTER = DateTimeFormatter.ofPattern("HHmmss").withZone(ZoneOffset.UTC)
        private val contentFileComparator = Comparator.comparing(ContentFile<*>::path, Comparators.charSequences())
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
    private val attrs = Attributes.of(ICEBERG_TABLE, table.name())

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
        if (lowSnapshot != null) checkPositionDelete()
        rewriteData()
        if (lowSnapshot != null) rewriteEqualityDelete()

        apply()
    }

    /**
     * @see org.apache.iceberg.AlluvialSquashOperation.validatePosDeletesReferenceToDataFileInRange
     */
    private fun checkPositionDelete() = tracer.withSpan("CompactSnapshots.checkPositionDelete", attrs) {
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

    private fun rewriteData() = tracer.withSpan("CompactSnapshots.rewriteData", attrs) {
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
    private fun planFileGroups(): Map<StructLike, List<FileScanTask>> = tracer.withSpan(
        "CompactSnapshots.planFileGroups", attrs
    ) {
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
    ) = tracer.withSpan(
        "CompactSnapshots.rewriteDataForPartition", attrs
    ) { span ->
        logger.info("rewrite DATA for partition={} with {} FileScanTasks", partition, files.size)
        span.setAttribute(ICEBERG_PARTITION, partition.toString())
        if (logger.isDebugEnabled) logging(files)
        val reader = GenericReader(io, schema)
        val iterable = reader.openTask(files)

        iterable.useResource {
            for (rec in it) {
                writer.write(rec, spec, partition)
            }
        }
    }

    private fun rewriteEqualityDelete() = tracer.withSpan("CompactSnapshots.rewriteEqualityDelete", attrs) {
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
    private fun planEqualityDeleteFiles(): Map<Set<Int>, Map<StructLike, List<DeleteFile>>> = tracer.withSpan(
        "CompactSnapshots.planEqualityDeleteFiles", attrs
    ) {
        val deleteEntries = highSnapshot.deleteManifests(io)
            .filter { it.hasAddedFiles() || it.hasExistingFiles() } // ignoreDeleted ManifestFile
            .transform {
                ManifestFiles.readDeleteManifest(it, io, specsById)
                    .liveEntries() // ignoreDeleted ManifestFile
            }
            .concat()
            .filterEntryAfter(lowSnapshot) // ignoreDeleted ManifestFile
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
    ) = tracer.withSpan(
        "CompactSnapshots.rewriteEqualityDeleteForPartition", attrs
    ) { span ->
        logger.info("rewrite EQUALITY_DELETES for partition={} with {} DeleteFiles", partition, files.size)
        span.setAttribute(ICEBERG_PARTITION, partition.toString())
        span.setAttribute(ICEBERG_SCHEMA, deleteSchema.toString())
        if (logger.isDebugEnabled) logging(files)
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
    private fun apply() = tracer.withSpan("CompactSnapshots.apply", attrs) {
        val txn = newTransaction()
        val squash = txn.squash()
        squash.squash(lowSnapshot?.snapshotId(), highSnapshot.snapshotId())
            .validateDeleteFilesInRange(false) // already validated

        val result = resultBuilder.build()
        result.dataFiles().forEach(squash::add)
        result.deleteFiles().forEach(squash::add)

        tracer.withSpan("CompactSnapshots.commit", attrs) {
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

    @JvmName("loggingFileScanTasks")
    private fun logging(tasks: List<FileScanTask>) {
        tasks.forEachIndexed { idx, task ->
            logger.debug(
                "#{} {}={} | filesCount={}, totalSizeBytes={}", idx, task.file().content(), task.file().path(),
                task.filesCount(), task.sizeBytes()
            )
            task.deletes().sortedWith(contentFileComparator).forEach {
                logger.debug("#{}  | {}={}", idx, it.content(), it.path())
            }
        }
    }

    @JvmName("loggingContentFiles")
    private fun logging(files: List<ContentFile<*>>) {
        files.sortedWith(contentFileComparator).forEach {
            logger.debug("{}={}", it.content(), it.path())
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
}
