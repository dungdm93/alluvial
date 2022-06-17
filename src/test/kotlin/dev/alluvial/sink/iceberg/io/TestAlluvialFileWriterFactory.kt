package dev.alluvial.sink.iceberg.io

import dev.alluvial.backport.iceberg.io.FileWriterFactory
import dev.alluvial.backport.iceberg.io.TestFileWriterFactory
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.source.kafka.structSchema
import org.apache.iceberg.FileFormat
import org.apache.iceberg.util.StructLikeSet
import org.apache.kafka.connect.data.Schema.INT32_SCHEMA
import org.apache.kafka.connect.data.Schema.STRING_SCHEMA

internal class TestAlluvialFileWriterFactory(fileFormat: FileFormat, partitioned: Boolean) :
    TestFileWriterFactory<KafkaStruct>(fileFormat, partitioned) {

    override fun newWriterFactory(
        dataSchema: IcebergSchema, equalityFieldIds: List<Int>?,
        equalityDeleteRowSchema: IcebergSchema?,
        positionDeleteRowSchema: IcebergSchema?
    ): FileWriterFactory<KafkaStruct> {
        return AlluvialFileWriterFactory.buildFor(table) {
            this.dataFileFormat = format()
            this.deleteFileFormat = format()
            this.equalityFieldIds = equalityFieldIds?.toIntArray() ?: intArrayOf()
            this.equalityDeleteRowSchema = equalityDeleteRowSchema
            this.positionDeleteRowSchema = positionDeleteRowSchema
        }
    }

    override fun toRow(id: Int, data: String): KafkaStruct {
        val schema = structSchema {
            name("test")
            field("id", INT32_SCHEMA)
            field("data", STRING_SCHEMA)
        }
        return KafkaStruct(schema)
            .put("id", id)
            .put("data", data)
    }

    override fun toSet(rows: Iterable<KafkaStruct>): StructLikeSet? {
        val schema = table.schema()
        val set = StructLikeSet.create(schema.asStruct())
        for (row in rows) {
            val wrapper = StructWrapper(row.schema(), schema)
            set.add(wrapper.wrap(row))
        }
        return set
    }
}
