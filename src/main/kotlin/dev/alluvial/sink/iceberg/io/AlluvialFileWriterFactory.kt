package dev.alluvial.sink.iceberg.io

import dev.alluvial.backport.iceberg.io.BaseFileWriterFactory
import dev.alluvial.sink.iceberg.avro.KafkaAvroWriter
import dev.alluvial.sink.iceberg.parquet.KafkaParquetWriter
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.toKafkaSchema
import org.apache.iceberg.FileFormat
import org.apache.iceberg.MetadataColumns.DELETE_FILE_ROW_FIELD_NAME
import org.apache.iceberg.SortOrder
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties
import org.apache.iceberg.avro.BackportAvro
import org.apache.iceberg.io.DeleteSchemaUtil
import org.apache.iceberg.orc.BackportORC
import org.apache.iceberg.parquet.BackportParquet
import org.apache.iceberg.relocated.com.google.common.base.Preconditions

class AlluvialFileWriterFactory private constructor(
    table: Table,
    dataFileFormat: FileFormat,
    dataSchema: IcebergSchema,
    private var dataKafkaSchema: KafkaSchema?,
    dataSortOrder: SortOrder,
    deleteFileFormat: FileFormat,
    equalityFieldIds: IntArray?,
    equalityDeleteRowSchema: IcebergSchema?,
    private var equalityDeleteKafkaSchema: KafkaSchema?,
    equalityDeleteSortOrder: SortOrder?,
    positionDeleteRowSchema: IcebergSchema?,
    private var positionDeleteKafkaSchema: KafkaSchema?,
) : BaseFileWriterFactory<KafkaStruct>(
    table, dataFileFormat, dataSchema, dataSortOrder, deleteFileFormat,
    equalityFieldIds, equalityDeleteRowSchema, equalityDeleteSortOrder,
    positionDeleteRowSchema
) {
    private fun dataKafkaSchema(): KafkaSchema {
        if (dataKafkaSchema == null) {
            val dataSchema = dataSchema()
            Preconditions.checkNotNull(dataSchema, "Data schema must not be null")
            dataKafkaSchema = dataSchema.toKafkaSchema()
        }
        return dataKafkaSchema!!
    }

    private fun equalityDeleteKafkaSchema(): KafkaSchema {
        if (equalityDeleteKafkaSchema == null) {
            val equalityDeleteRowSchema = equalityDeleteRowSchema()
            Preconditions.checkNotNull(equalityDeleteRowSchema, "Equality delete row schema shouldn't be null")
            equalityDeleteKafkaSchema = equalityDeleteRowSchema.toKafkaSchema()
        }
        return equalityDeleteKafkaSchema!!
    }

    private fun positionDeleteKafkaSchema(): KafkaSchema {
        if (positionDeleteKafkaSchema == null) {
            val positionDeleteRowSchema = positionDeleteRowSchema()
            // wrap the optional row schema into the position delete schema that contains path and position
            val positionDeleteSchema = DeleteSchemaUtil.posDeleteSchema(positionDeleteRowSchema)
            positionDeleteKafkaSchema = positionDeleteSchema.toKafkaSchema()
        }
        return positionDeleteKafkaSchema!!
    }

    override fun configureDataWrite(builder: BackportAvro.DataWriteBuilder) {
        builder.createWriterFunc { KafkaAvroWriter(dataKafkaSchema()) }
    }

    override fun configureDataWrite(builder: BackportParquet.DataWriteBuilder) {
        builder.createWriterFunc { msgType ->
            KafkaParquetWriter.buildWriter(dataKafkaSchema(), msgType)
        }
    }

    override fun configureDataWrite(builder: BackportORC.DataWriteBuilder) {
        TODO("Not yet implemented")
    }

    override fun configureEqualityDelete(builder: BackportAvro.DeleteWriteBuilder) {
        builder.createWriterFunc { KafkaAvroWriter(equalityDeleteKafkaSchema()) }
    }

    override fun configureEqualityDelete(builder: BackportParquet.DeleteWriteBuilder) {
        builder.createWriterFunc { msgType ->
            KafkaParquetWriter.buildWriter(equalityDeleteKafkaSchema(), msgType)
        }
    }

    override fun configureEqualityDelete(builder: BackportORC.DeleteWriteBuilder) {
        TODO("Not yet implemented")
    }

    override fun configurePositionDelete(builder: BackportAvro.DeleteWriteBuilder) {
        val rowField = positionDeleteKafkaSchema().field(DELETE_FILE_ROW_FIELD_NAME)
        if (rowField != null) {
            // KafkaAvroWriter accepts just the Kafka schema of the field ignoring the path and pos
            builder.createWriterFunc { KafkaAvroWriter(rowField.schema()) }
        }
    }

    override fun configurePositionDelete(builder: BackportParquet.DeleteWriteBuilder) {
        builder.createWriterFunc { msgType ->
            KafkaParquetWriter.buildWriter(positionDeleteKafkaSchema(), msgType)
        }
    }

    override fun configurePositionDelete(builder: BackportORC.DeleteWriteBuilder) {
        TODO("Not yet implemented")
    }

    data class Builder internal constructor(val table: Table) {
        var dataFileFormat: FileFormat
        var dataSchema: IcebergSchema
        var dataKafkaSchema: KafkaSchema? = null
        var dataSortOrder: SortOrder
        var deleteFileFormat: FileFormat
        var equalityFieldIds: IntArray
        var equalityDeleteRowSchema: IcebergSchema? = null
        var equalityDeleteKafkaSchema: KafkaSchema? = null
        var equalityDeleteSortOrder: SortOrder? = null
        var positionDeleteRowSchema: IcebergSchema? = null
        var positionDeleteKafkaSchema: KafkaSchema? = null

        init {
            val properties = table.properties()

            val dataFileFormatName = properties.getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT,
                TableProperties.DEFAULT_FILE_FORMAT_DEFAULT
            )
            dataFileFormat = FileFormat.valueOf(dataFileFormatName.uppercase())

            val deleteFileFormatName = properties.getOrDefault(
                TableProperties.DELETE_DEFAULT_FILE_FORMAT,
                dataFileFormatName
            )
            deleteFileFormat = FileFormat.valueOf(deleteFileFormatName.uppercase())

            dataSchema = table.schema()
            dataSortOrder = table.sortOrder()

            val identifierFieldIds = table.schema().identifierFieldIds()
            equalityFieldIds = identifierFieldIds.toIntArray()

            val identifierFieldNames = table.schema().identifierFieldNames()
            equalityDeleteRowSchema = table.schema().select(identifierFieldNames)
        }

        fun build() = AlluvialFileWriterFactory(
            table, dataFileFormat, dataSchema, dataKafkaSchema, dataSortOrder, deleteFileFormat,
            equalityFieldIds, equalityDeleteRowSchema, equalityDeleteKafkaSchema, equalityDeleteSortOrder,
            positionDeleteRowSchema, positionDeleteKafkaSchema,
        )
    }

    companion object {
        fun buildFor(table: Table) =
            Builder(table).build()

        fun buildFor(table: Table, block: Builder.() -> Unit) =
            Builder(table).apply(block).build()
    }
}
