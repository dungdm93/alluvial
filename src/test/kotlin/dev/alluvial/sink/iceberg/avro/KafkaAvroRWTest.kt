package dev.alluvial.sink.iceberg.avro

import dev.alluvial.sink.iceberg.type.AssertionsKafka.assertEquals
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
import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.data.Record as IcebergRecord
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

internal class KafkaAvroRWTest : DataTest() {
    override fun writeAndValidate(icebergSchema: IcebergSchema) {
        val kafkaSchema = icebergSchema.toKafkaSchema()
        val expectedIcebergRecords = RandomGenericData.generate(icebergSchema, 100, 0L).toList()
        val expectedKafkaStructs = RandomKafkaStruct.convert(icebergSchema, expectedIcebergRecords).toList()

        validateKafkaReader(icebergSchema, kafkaSchema, expectedIcebergRecords)
        validateKafkaWriter(icebergSchema, kafkaSchema, expectedKafkaStructs)
    }

    private fun writeAndValidate(kafkaSchema: KafkaSchema) {
        val icebergSchema = kafkaSchema.toIcebergSchema()
        val expectedKafkaStructs = RandomKafkaStruct.generate(kafkaSchema, 100, 5).toList()

        val expectedIcebergRecords = validateKafkaWriter(icebergSchema, kafkaSchema, expectedKafkaStructs)
        validateKafkaReader(icebergSchema, kafkaSchema, expectedIcebergRecords)
    }

    private fun validateKafkaReader(
        icebergSchema: IcebergSchema,
        kafkaSchema: KafkaSchema,
        expected: List<IcebergRecord>
    ): List<KafkaStruct> {
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
            .createReaderFunc { _: IcebergSchema, readSchema: AvroSchema ->
                KafkaAvroReader(kafkaSchema, readSchema)
            }
            .build()
        val actual = reader.use {
            it.toList()
        }

        expected.zip(actual) { e, a ->
            assertEquals(icebergSchema, kafkaSchema, e, a)
        }

        return actual
    }

    private fun validateKafkaWriter(
        icebergSchema: IcebergSchema,
        kafkaSchema: KafkaSchema,
        expected: List<KafkaStruct>,
    ): List<IcebergRecord> {
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
            assertEquals(icebergSchema, kafkaSchema, a, e)
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
