package dev.alluvial.sink.iceberg.avro

import dev.alluvial.sink.iceberg.type.AssertionsKafka.assertEquals
import dev.alluvial.sink.iceberg.type.IcebergRecord
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.RandomKafkaStruct
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import dev.alluvial.sink.iceberg.type.toKafkaSchema
import dev.alluvial.source.kafka.DEBEZIUM_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_PRIMITIVES_SCHEMA
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
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.Test

internal class KafkaAvroRWTest : DataTest() {
    override fun writeAndValidate(iSchema: IcebergSchema) {
        val sSchema = iSchema.toKafkaSchema()
        val expectedIcebergRecords = RandomGenericData.generate(iSchema, 100, 0L).toList()
        val expectedKafkaStructs = RandomKafkaStruct.convert(iSchema, expectedIcebergRecords).toList()

        validateKafkaReader(iSchema, sSchema, expectedIcebergRecords)
        validateKafkaWriter(iSchema, sSchema, expectedKafkaStructs)
    }

    private fun writeAndValidate(sSchema: KafkaSchema) {
        val iSchema = sSchema.toIcebergSchema()
        val expectedKafkaStructs = RandomKafkaStruct.generate(sSchema, 100, 5).toList()

        val expectedIcebergRecords = validateKafkaWriter(iSchema, sSchema, expectedKafkaStructs)
        validateKafkaReader(iSchema, sSchema, expectedIcebergRecords)
    }

    private fun validateKafkaReader(
        iSchema: IcebergSchema,
        sSchema: KafkaSchema,
        expected: List<IcebergRecord>
    ): List<KafkaStruct> {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }

        // Write the expected records into AVRO file, then read them into Kafka Struct and assert with the expected Record list.
        val writer: FileAppender<IcebergRecord> = Avro.write(Files.localOutput(tmp))
            .schema(iSchema)
            .createWriterFunc { DataWriter.create<DatumWriter<*>>(it) }
            .named("test")
            .build()

        writer.use {
            expected.forEach(it::add)
        }

        val reader: CloseableIterable<KafkaStruct> = Avro.read(Files.localInput(tmp))
            .project(iSchema)
            .createReaderFunc { _, readSchema ->
                KafkaAvroReader(sSchema, readSchema)
            }
            .build()
        val actual = reader.use {
            it.toList()
        }

        expected.zip(actual) { e, a ->
            assertEquals(iSchema, sSchema, e, a)
        }

        return actual
    }

    private fun validateKafkaWriter(
        iSchema: IcebergSchema,
        sSchema: KafkaSchema,
        expected: List<KafkaStruct>,
    ): List<IcebergRecord> {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }

        // Write the expected Kafka Struct into AVRO file, then read them into Record and assert with the expected Kafka Struct list.
        val writer: FileAppender<KafkaStruct> = Avro.write(Files.localOutput(tmp))
            .schema(iSchema)
            .createWriterFunc { KafkaAvroWriter(sSchema) }
            .named("test")
            .build()

        writer.use {
            expected.forEach(writer::add)
        }

        val reader: AvroIterable<IcebergRecord> = Avro.read(Files.localInput(tmp))
            .project(iSchema)
            .createReaderFunc { expectedSchema, readSchema ->
                DataReader.create<DatumReader<*>>(expectedSchema, readSchema)
            }
            .build()
        val actual = reader.toList()

        actual.zip(expected) { a, e ->
            assertEquals(iSchema, sSchema, a, e)
        }

        return actual
    }

    @Test
    fun testKafkaPrimitives() {
        val kafkaSchema = SchemaBuilder.struct().apply {
            KAFKA_PRIMITIVES_SCHEMA.forEach(::field)
        }.build()
        writeAndValidate(kafkaSchema)
    }

    @Test
    fun testKafkaLogicalTypes() {
        val kafkaSchema = SchemaBuilder.struct().apply {
            KAFKA_LOGICAL_TYPES_SCHEMA.forEach(::field)
        }.build()
        writeAndValidate(kafkaSchema)
    }

    @Test
    fun testDebeziumLogicalTypes() {
        val kafkaSchema = SchemaBuilder.struct().apply {
            DEBEZIUM_LOGICAL_TYPES_SCHEMA.forEach(::field)
        }.build()
        writeAndValidate(kafkaSchema)
    }
}
