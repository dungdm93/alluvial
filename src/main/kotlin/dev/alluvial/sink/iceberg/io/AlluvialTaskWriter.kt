package dev.alluvial.sink.iceberg.io

import dev.alluvial.backport.iceberg.io.PartitioningWriterFactory
import dev.alluvial.backport.iceberg.io.PathOffset
import dev.alluvial.backport.iceberg.io.StructCopy
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import org.apache.iceberg.ContentFile
import org.apache.iceberg.PartitionKey
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.StructLike
import org.apache.iceberg.deletes.PositionDelete
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.io.TaskWriter
import org.apache.iceberg.io.WriteResult
import org.apache.iceberg.util.StructLikeMap
import org.apache.iceberg.util.Tasks
import org.apache.kafka.connect.sink.SinkRecord

class AlluvialTaskWriter(
    partitioningWriterFactory: PartitioningWriterFactory<KafkaStruct>,
    private val spec: PartitionSpec,
    private val io: FileIO,
    private val partitioner: Partitioner<KafkaStruct>,
    sSchema: KafkaSchema,
    iSchema: IcebergSchema,
    equalityFieldIds: Set<Int>
) : TaskWriter<SinkRecord> {
    private val insertWriter = partitioningWriterFactory.newDataWriter()
    private val equalityDeleteWriter = partitioningWriterFactory.newEqualityDeleteWriter()
    private val positionDeleteWriter = partitioningWriterFactory.newPositionDeleteWriter()
    private val positionDelete: PositionDelete<KafkaStruct> = PositionDelete.create()
    private val insertedRowMap: MutableMap<StructLike, PathOffset>
    private val keyer: Keyer<SinkRecord>

    private var key: StructLike? = null

    init {
        val equalityFieldNames = equalityFieldIds.map { iSchema.findField(it).name() }
        val iKeySchema = iSchema.select(equalityFieldNames)
        keyer = keyerFor(sSchema, iKeySchema)
        insertedRowMap = StructLikeMap.create(iKeySchema.asStruct())
    }

    override fun write(record: SinkRecord) {
        val value = record.value() as? KafkaStruct ?: return // Tombstone events
        val before = value.getStruct("before")
        val after = value.getStruct("after")
        key = keyer(record)

        when (value.getString("op")) {
            // read (snapshot) events
            "r" -> {
                delete(after) // ensure no duplicate data when re-snapshot
                insert(after)
            }
            // create events
            "c" -> insert(after)
            // update events
            "u" -> {
                delete(before)
                insert(after)
            }
            // delete events
            "d" -> delete(before)
            else -> {} // ignore
        }
    }

    private fun internalPosDelete(key: StructLike, partition: StructLike?): Boolean {
        val previous = insertedRowMap.remove(key)
        if (previous != null) {
            positionDeleteWriter.write(previous.setTo(positionDelete), spec, partition)
            return true
        }
        return false
    }

    private fun insert(row: KafkaStruct) {
        val partition = partitioner(row)
        val copiedKey = StructCopy.copy(key)

        internalPosDelete(copiedKey, partition)
        val pathOffset = insertWriter.write(row, spec, partition)
        insertedRowMap[copiedKey] = pathOffset
    }

    private fun delete(row: KafkaStruct) {
        val partition = partitioner(row)
        val copiedKey = StructCopy.copy(key)

        if (!internalPosDelete(copiedKey, partition)) {
            equalityDeleteWriter.write(row, spec, partition)
        }
    }

    override fun abort() {
        close()

        // clean up files created by this writer
        val files = with(writeResult()) {
            buildList {
                addAll(dataFiles())
                addAll(deleteFiles())
            }
        }
        Tasks.foreach(files)
            .throwFailureWhenFinished()
            .noRetry()
            .run { file: ContentFile<*> -> io.deleteFile(file.path().toString()) }
    }

    override fun complete(): WriteResult {
        close()
        return writeResult()
    }

    override fun close() {
        insertWriter.close()
        equalityDeleteWriter.close()
        positionDeleteWriter.close()
    }

    private fun writeResult(): WriteResult {
        val insertResult = insertWriter.result()
        val equalityDeleteResult = equalityDeleteWriter.result()
        val positionDeleteResult = positionDeleteWriter.result()

        return WriteResult.builder()
            .addDataFiles(insertResult.dataFiles())
            .addDeleteFiles(equalityDeleteResult.deleteFiles())
            .addDeleteFiles(positionDeleteResult.deleteFiles())
            .addReferencedDataFiles(positionDeleteResult.referencedDataFiles())
            .build()
    }

    companion object {
        val unpartition: Partitioner<KafkaStruct> = { _ -> null }

        fun partitionerFor(
            spec: PartitionSpec,
            sSchema: KafkaSchema,
            iSchema: IcebergSchema,
        ): Partitioner<KafkaStruct> {
            return object : Partitioner<KafkaStruct> {
                private val wrapper: StructWrapper = StructWrapper(sSchema, iSchema)
                private val partitionKey = PartitionKey(spec, iSchema)

                override fun invoke(record: KafkaStruct): StructLike {
                    partitionKey.partition(wrapper.wrap(record))
                    return partitionKey
                }
            }
        }

        fun keyerFor(sSchema: KafkaSchema, iSchema: IcebergSchema): Keyer<SinkRecord> {
            return object : Keyer<SinkRecord> {
                private val wrapper: StructWrapper = StructWrapper(sSchema, iSchema)

                override fun invoke(record: SinkRecord): StructLike {
                    val key = record.key() as KafkaStruct
                    return wrapper.wrap(key)
                }
            }
        }
    }
}
