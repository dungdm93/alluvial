package dev.alluvial.sink.iceberg.data.parquet

import org.apache.iceberg.Files
import org.apache.iceberg.Schema
import org.apache.iceberg.data.DataTest
import org.apache.iceberg.data.DataTestHelpers.assertEquals
import org.apache.iceberg.data.RandomGenericData
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.parquet.Parquet

internal class GenericParquetRWTest : DataTest() {
    override fun writeAndValidate(schema: Schema) {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }
        val expected = RandomGenericData.generate(schema, 100, 0L).toList()

        val writer: FileAppender<Record> = Parquet.write(Files.localOutput(tmp))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .named("test")
            .build()

        writer.use {
            expected.forEach(it::add)
        }

        val reader: CloseableIterable<Record> = Parquet.read(Files.localInput(tmp))
            .project(schema)
            .createReaderFunc { fileSchema ->
                GenericParquetReaders.buildReader(schema, fileSchema)
            }
            .build()
        val actual = reader.use {
            it.toList()
        }

        expected.zip(actual) { e, a ->
            assertEquals(schema.asStruct(), e, a)
        }
    }
}
