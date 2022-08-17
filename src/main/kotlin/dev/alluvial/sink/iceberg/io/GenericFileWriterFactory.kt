package dev.alluvial.sink.iceberg.io

import dev.alluvial.sink.iceberg.type.IcebergSchema
import org.apache.iceberg.FileFormat
import org.apache.iceberg.SortOrder
import org.apache.iceberg.Table
import org.apache.iceberg.TableProperties
import org.apache.iceberg.avro.Avro
import org.apache.iceberg.data.BaseFileWriterFactory
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.avro.DataWriter
import org.apache.iceberg.data.orc.GenericOrcWriter
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.orc.ORC
import org.apache.iceberg.parquet.Parquet

class GenericFileWriterFactory private constructor(
    table: Table,
    dataFileFormat: FileFormat,
    dataSchema: IcebergSchema,
    dataSortOrder: SortOrder,
    deleteFileFormat: FileFormat,
    equalityFieldIds: IntArray?,
    equalityDeleteRowSchema: IcebergSchema?,
    equalityDeleteSortOrder: SortOrder?,
    positionDeleteRowSchema: IcebergSchema?,
) : BaseFileWriterFactory<Record>(
    table, dataFileFormat, dataSchema, dataSortOrder, deleteFileFormat,
    equalityFieldIds, equalityDeleteRowSchema, equalityDeleteSortOrder,
    positionDeleteRowSchema
) {
    override fun configureDataWrite(builder: Avro.DataWriteBuilder) {
        builder.createWriterFunc { DataWriter.create<Record>(it) }
    }

    override fun configureDataWrite(builder: Parquet.DataWriteBuilder) {
        builder.createWriterFunc(GenericParquetWriter::buildWriter)
    }

    override fun configureDataWrite(builder: ORC.DataWriteBuilder) {
        builder.createWriterFunc(GenericOrcWriter::buildWriter)
    }

    override fun configureEqualityDelete(builder: Avro.DeleteWriteBuilder) {
        builder.createWriterFunc { DataWriter.create<Record>(it) }
    }

    override fun configureEqualityDelete(builder: Parquet.DeleteWriteBuilder) {
        builder.createWriterFunc(GenericParquetWriter::buildWriter)
    }

    override fun configureEqualityDelete(builder: ORC.DeleteWriteBuilder) {
        builder.createWriterFunc(GenericOrcWriter::buildWriter)
    }

    override fun configurePositionDelete(builder: Avro.DeleteWriteBuilder) {
        builder.createWriterFunc { DataWriter.create<Record>(it) }
    }

    override fun configurePositionDelete(builder: Parquet.DeleteWriteBuilder) {
        builder.createWriterFunc(GenericParquetWriter::buildWriter)
    }

    override fun configurePositionDelete(builder: ORC.DeleteWriteBuilder) {
        builder.createWriterFunc(GenericOrcWriter::buildWriter)
    }

    data class Builder internal constructor(val table: Table) {
        var dataFileFormat: FileFormat
        var dataSchema: IcebergSchema
        var dataSortOrder: SortOrder
        var deleteFileFormat: FileFormat
        var equalityFieldIds: IntArray
        var equalityDeleteRowSchema: IcebergSchema? = null
        var equalityDeleteSortOrder: SortOrder? = null
        var positionDeleteRowSchema: IcebergSchema? = null

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

        fun build() = GenericFileWriterFactory(
            table,
            dataFileFormat, dataSchema, dataSortOrder, deleteFileFormat,
            equalityFieldIds, equalityDeleteRowSchema, equalityDeleteSortOrder,
            positionDeleteRowSchema
        )
    }

    companion object {
        fun builder(table: Table) =
            Builder(table)

        fun buildFor(table: Table) =
            Builder(table).build()

        fun buildFor(table: Table, block: Builder.() -> Unit) =
            Builder(table).apply(block).build()
    }
}
