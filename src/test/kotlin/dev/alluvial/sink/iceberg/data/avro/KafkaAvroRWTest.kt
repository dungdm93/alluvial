package dev.alluvial.sink.iceberg.data.avro

import dev.alluvial.sink.iceberg.data.AssertionsKafka.assertEquals
import dev.alluvial.sink.iceberg.data.KafkaSchemaUtil
import dev.alluvial.sink.iceberg.data.RandomKafkaStruct
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.iceberg.Files
import org.apache.iceberg.avro.Avro
import org.apache.iceberg.avro.AvroIterable
import org.apache.iceberg.data.DataTest
import org.apache.iceberg.data.RandomGenericData
import org.apache.iceberg.data.avro.DataReader
import org.apache.iceberg.data.avro.DataWriter
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.FileAppender
import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.data.Record as IcebergRecord
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

internal class KafkaAvroRWTest : DataTest() {
    override fun writeAndValidate(icebergSchema: IcebergSchema) {
        val kafkaSchema = KafkaSchemaUtil.toKafkaSchema(icebergSchema)
        val expectedIcebergRecords = RandomGenericData.generate(icebergSchema, 3, 0L).toList()
        val expectedKafkaStructs = RandomKafkaStruct.convert(icebergSchema, expectedIcebergRecords).toList()

        validateKafkaReader(icebergSchema, expectedIcebergRecords)
        validateKafkaWriter(icebergSchema, kafkaSchema, expectedKafkaStructs)
    }

    private fun validateKafkaReader(
        icebergSchema: IcebergSchema,
        expected: List<IcebergRecord>
    ) {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }

        // Write the expected records into AVRO file, then read them into Kafka Struct and assert with the expected Record list.
        val writer: FileAppender<IcebergRecord> = Avro.write(Files.localOutput(tmp))
            .schema(icebergSchema)
            .createWriterFunc { DataWriter.create<DatumWriter<*>>(it) }
            .named("test")
            .build()

        writer.use {
            expected.forEach(it::add)
        }

        val reader: CloseableIterable<KafkaStruct> = Avro.read(Files.localInput(tmp))
            .project(icebergSchema)
            .createReaderFunc { expectedSchema: IcebergSchema, readSchema: AvroSchema ->
                KafkaAvroReader(expectedSchema, readSchema)
            }
            .build()
        val actual = reader.use {
            it.toList()
        }

        expected.zip(actual) { e, a ->
            assertEquals(icebergSchema, e, a)
        }
    }

    private fun validateKafkaWriter(
        icebergSchema: IcebergSchema,
        kafkaSchema: KafkaSchema,
        expected: List<KafkaStruct>
    ) {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }

        // Write the expected Kafka Struct into AVRO file, then read them into Record and assert with the expected Kafka Struct list.
        val writer: FileAppender<KafkaStruct> = Avro.write(Files.localOutput(tmp))
            .schema(icebergSchema)
            .createWriterFunc { KafkaAvroWriter(kafkaSchema) }
            .named("test")
            .build()

        writer.use {
            expected.forEach(writer::add)
        }

        val reader: AvroIterable<IcebergRecord> = Avro.read(Files.localInput(tmp))
            .project(icebergSchema)
            .createReaderFunc { expectedSchema: IcebergSchema, readSchema: AvroSchema ->
                DataReader.create<DatumReader<*>>(expectedSchema, readSchema)
            }
            .build()
        val actual = reader.toList()

        actual.zip(expected) { a, e ->
            assertEquals(icebergSchema, a, e)
        }
    }
}
