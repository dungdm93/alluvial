package dev.alluvial.sink.iceberg.data.parquet

import dev.alluvial.sink.iceberg.data.AssertionsKafka.assertEquals
import dev.alluvial.sink.iceberg.data.KafkaSchemaUtil
import dev.alluvial.sink.iceberg.data.RandomKafkaStruct
import dev.alluvial.source.kafka.DEBEZIUM_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_LOGICAL_TYPES_SCHEMA
import dev.alluvial.source.kafka.KAFKA_PRIMITIVES_SCHEMA
import org.apache.iceberg.Files
import org.apache.iceberg.data.DataTest
import org.apache.iceberg.data.RandomGenericData
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.parquet.Parquet
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.Test
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.data.Record as IcebergRecord
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

internal class KafkaParquetRWTest : DataTest() {
    override fun writeAndValidate(icebergSchema: IcebergSchema) {
        val kafkaSchema = KafkaSchemaUtil.toKafkaSchema(icebergSchema)
        val expectedIcebergRecords = RandomGenericData.generate(icebergSchema, 100, 0L).toList()
        val expectedKafkaStructs = RandomKafkaStruct.convert(icebergSchema, expectedIcebergRecords).toList()

        validateKafkaReader(icebergSchema, kafkaSchema, expectedIcebergRecords)
        validateKafkaWriter(icebergSchema, kafkaSchema, expectedKafkaStructs)
    }

    private fun writeAndValidate(kafkaSchema: KafkaSchema) {
        val icebergSchema = KafkaSchemaUtil.toIcebergSchema(kafkaSchema)
        val expectedKafkaStructs = RandomKafkaStruct.generate(kafkaSchema, 100, 5).toList()

        val expectedIcebergRecords = validateKafkaWriter(icebergSchema, kafkaSchema, expectedKafkaStructs)
        validateKafkaReader(icebergSchema, kafkaSchema, expectedIcebergRecords)
    }

    private fun validateKafkaReader(
        icebergSchema: IcebergSchema,
        kafkaSchema: KafkaSchema,
        expected: List<IcebergRecord>
    ) {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }

        // Write the expected records into Parquet file, then read them into Kafka Struct and assert with the expected Record list.
        val writer: FileAppender<IcebergRecord> = Parquet.write(Files.localOutput(tmp))
            .schema(icebergSchema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .named("test")
            .build()

        writer.use {
            expected.forEach(it::add)
        }

        val reader: CloseableIterable<KafkaStruct> = Parquet.read(Files.localInput(tmp))
            .project(icebergSchema)
            .createReaderFunc { fileSchema ->
                KafkaParquetReader.buildReader(kafkaSchema, fileSchema)
            }
            .build()
        val actual = reader.use {
            it.toList()
        }

        expected.zip(actual) { e, a ->
            assertEquals(icebergSchema, kafkaSchema, e, a)
        }
    }

    private fun validateKafkaWriter(
        icebergSchema: IcebergSchema,
        kafkaSchema: KafkaSchema,
        expected: List<KafkaStruct>
    ): List<IcebergRecord> {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }

        // Write the expected Kafka Struct into Parquet file, then read them into Record and assert with the expected Kafka Struct list.
        val writer: FileAppender<KafkaStruct> = Parquet.write(Files.localOutput(tmp))
            .schema(icebergSchema)
            .createWriterFunc { msgType ->
                KafkaParquetWriter.buildWriter(kafkaSchema, msgType)
            }
            .named("test")
            .build()

        writer.use {
            expected.forEach(writer::add)
        }

        val reader: CloseableIterable<IcebergRecord> = Parquet.read(Files.localInput(tmp))
            .project(icebergSchema)
            .createReaderFunc { fileSchema ->
                GenericParquetReaders.buildReader(icebergSchema, fileSchema)
            }
            .build()
        val actual = reader.use {
            it.toList()
        }

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
