package dev.alluvial.sink.iceberg.data.avro

import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.iceberg.Files
import org.apache.iceberg.Schema
import org.apache.iceberg.avro.Avro
import org.apache.iceberg.data.DataTest
import org.apache.iceberg.data.DataTestHelpers.assertEquals
import org.apache.iceberg.data.RandomGenericData
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.avro.DataReader
import org.apache.iceberg.data.avro.DataWriter
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.FileAppender
import org.apache.avro.Schema as AvroSchema

internal class GenericAvroRWTest : DataTest() {
    override fun writeAndValidate(schema: Schema) {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }
        val expected = RandomGenericData.generate(schema, 100, 0L).toList()

        val writer: FileAppender<Record> = Avro.write(Files.localOutput(tmp))
            .schema(schema)
            .createWriterFunc { DataWriter.create<DatumWriter<*>>(it) }
            .named("test")
            .build()

        writer.use {
            expected.forEach(it::add)
        }

        val reader: CloseableIterable<Record> = Avro.read(Files.localInput(tmp))
            .project(schema)
            .createReaderFunc { expectedSchema: Schema, readSchema: AvroSchema ->
                DataReader.create<DatumReader<*>>(expectedSchema, readSchema)
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
