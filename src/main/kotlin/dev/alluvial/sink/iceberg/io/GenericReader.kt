package dev.alluvial.sink.iceberg.io

import dev.alluvial.sink.iceberg.concat
import dev.alluvial.sink.iceberg.filter
import dev.alluvial.sink.iceberg.transform
import org.apache.avro.io.DatumReader
import org.apache.iceberg.CombinedScanTask
import org.apache.iceberg.ContentFile
import org.apache.iceberg.FileFormat
import org.apache.iceberg.FileScanTask
import org.apache.iceberg.MetadataColumns
import org.apache.iceberg.Schema
import org.apache.iceberg.avro.Avro
import org.apache.iceberg.data.GenericDeleteFilter
import org.apache.iceberg.data.IdentityPartitionConverters
import org.apache.iceberg.data.InternalRecordWrapper
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.avro.DataReader
import org.apache.iceberg.data.orc.GenericOrcReader
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.expressions.Evaluator
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.orc.ORC
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.relocated.com.google.common.collect.Sets
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.util.PartitionUtil
import org.slf4j.LoggerFactory
import java.io.Serializable

/**
 * Copy of `org.apache.iceberg.data.GenericReader` with some modification
 * @see org.apache.iceberg.data.GenericReader
 * @see org.apache.iceberg.data.DeleteFilter.openDeletes
 */
@Suppress("MemberVisibilityCanBePrivate")
class GenericReader(
    private val io: FileIO,
    private val schema: Schema,
    private val caseSensitive: Boolean = true,
    private val reuseContainers: Boolean = false,
) : Serializable {
    companion object {
        private val logger = LoggerFactory.getLogger(GenericReader::class.java)
    }

    fun openGroup(groups: Iterable<CombinedScanTask>): CloseableIterable<Record> {
        val fileTasks = groups.transform(CombinedScanTask::files)
            .concat()
        return openTask(fileTasks)
    }

    fun openGroup(group: CombinedScanTask): CloseableIterable<Record> {
        val fileTasks = group.files()
        return openTask(fileTasks)
    }

    fun openTask(tasks: Iterable<FileScanTask>): CloseableIterable<Record> {
        return tasks.transform(this::openTask)
            .concat()
    }

    fun openTask(task: FileScanTask): CloseableIterable<Record> {
        val deletes = GenericDeleteFilter(io, task, schema, schema)
        val readSchema = deletes.requiredSchema()

        var records = open(task, readSchema)
        records = deletes.filter(records)
        records = applyResidual(records, readSchema, task.residual())

        return records
    }

    fun openFile(files: Iterable<ContentFile<*>>, filter: Expression): CloseableIterable<Record> {
        return files.transform { openFile(it, filter) }
            .concat()
    }

    fun openFile(files: Iterable<ContentFile<*>>): CloseableIterable<Record> {
        return files.transform(this::openFile)
            .concat()
    }

    fun openFile(file: ContentFile<*>, filter: Expression): CloseableIterable<Record> {
        return open(file, schema, filter)
    }

    fun openFile(file: ContentFile<*>): CloseableIterable<Record> {
        return open(file, schema, Expressions.alwaysTrue())
    }

    private fun applyResidual(
        records: CloseableIterable<Record>,
        recordSchema: Schema,
        residual: Expression?
    ): CloseableIterable<Record> {
        if (residual != null && residual !== Expressions.alwaysTrue()) {
            val wrapper = InternalRecordWrapper(recordSchema.asStruct())
            val filter = Evaluator(recordSchema.asStruct(), residual, caseSensitive)
            return records.filter { filter.eval(wrapper.wrap(it)) }
        }
        return records
    }

    private fun open(task: FileScanTask, fileProjection: Schema): CloseableIterable<Record> {
        val file = task.file()
        val input = io.newInputFile(file.path().toString())
        val partition = PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant)

        if (logger.isDebugEnabled) {
            logger.debug("Open FileScanTask {}({}): {}", file.content(), file.format(), file.path())
            task.deletes().forEach {
                logger.debug("\t{}({}): {}", it.content(), it.format(), it.path())
            }
        }
        return when (file.format()) {
            FileFormat.AVRO -> {
                if (task.residual() == Expressions.alwaysFalse()) return CloseableIterable.empty()

                val records = Avro.read(input)
                    .project(fileProjection)
                    .createReaderFunc { fileSchema ->
                        DataReader.create<DatumReader<*>>(fileProjection, fileSchema, partition)
                    }
                    .split(task.start(), task.length())
                    .reuseContainers(reuseContainers)
                    .build<Record>()

                return if (task.residual() == Expressions.alwaysTrue()) {
                    records
                } else {
                    val evaluator = Evaluator(fileProjection.asStruct(), task.residual())
                    records.filter { evaluator.eval(it) }
                }
            }

            FileFormat.PARQUET -> Parquet.read(input)
                .project(fileProjection)
                .createReaderFunc { fileSchema ->
                    GenericParquetReaders.buildReader(fileProjection, fileSchema, partition)
                }
                .split(task.start(), task.length())
                .filter(task.residual())
                .reuseContainers(reuseContainers)
                .build()

            FileFormat.ORC -> {
                val projectionWithoutConstantAndMetadataFields = TypeUtil.selectNot(
                    fileProjection,
                    Sets.union(partition.keys, MetadataColumns.metadataFieldIds())
                )
                ORC.read(input)
                    .project(projectionWithoutConstantAndMetadataFields)
                    .createReaderFunc { fileSchema ->
                        GenericOrcReader.buildReader(fileProjection, fileSchema, partition)
                    }
                    .split(task.start(), task.length())
                    .filter(task.residual())
                    .build()
            }

            else -> throw UnsupportedOperationException("Cannot read ${file.content()} in ${file.format()} file: ${file.path()}")
        }
    }

    private fun open(file: ContentFile<*>, fileProjection: Schema, filter: Expression): CloseableIterable<Record> {
        val input = io.newInputFile(file.path().toString())

        logger.debug("Open ContentFile {}({}): {}", file.content(), file.format(), file.path())
        return when (file.format()) {
            FileFormat.AVRO -> {
                if (filter == Expressions.alwaysFalse()) return CloseableIterable.empty()

                val records = Avro.read(input)
                    .project(fileProjection)
                    .reuseContainers(reuseContainers)
                    .createReaderFunc { fileSchema ->
                        DataReader.create<DatumReader<*>>(fileProjection, fileSchema)
                    }
                    .build<Record>()

                return if (filter == Expressions.alwaysTrue()) {
                    records
                } else {
                    val evaluator = Evaluator(fileProjection.asStruct(), filter)
                    records.filter { evaluator.eval(it) }
                }
            }

            FileFormat.PARQUET -> Parquet.read(input)
                .project(fileProjection)
                .reuseContainers(reuseContainers)
                .createReaderFunc { fileSchema ->
                    GenericParquetReaders.buildReader(fileProjection, fileSchema)
                }
                .filter(filter)
                .build()

            // Reusing containers is automatic for ORC. No need to set 'reuseContainers' here.
            FileFormat.ORC -> ORC.read(input)
                .project(fileProjection)
                .createReaderFunc { fileSchema ->
                    GenericOrcReader.buildReader(fileProjection, fileSchema)
                }
                .filter(filter)
                .build()

            else -> throw UnsupportedOperationException("Cannot read ${file.content()} in ${file.format()} file: ${file.path()}")
        }
    }

    private fun Parquet.ReadBuilder.reuseContainers(shouldReuse: Boolean): Parquet.ReadBuilder {
        if (shouldReuse) {
            this.reuseContainers()
        }
        return this
    }
}
