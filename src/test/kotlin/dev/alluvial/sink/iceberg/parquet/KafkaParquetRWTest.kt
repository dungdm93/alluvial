package dev.alluvial.sink.iceberg.parquet

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
import dev.alluvial.source.kafka.structSchema
import org.apache.iceberg.Files
import org.apache.iceberg.data.DataTest
import org.apache.iceberg.data.RandomGenericData
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.parquet.Parquet
import org.junit.Test

internal open class KafkaParquetRWTest : DataTest() {
    override fun writeAndValidate(iSchema: IcebergSchema) {
        val sSchema = convert(iSchema)
        val expectedIcebergRecords = RandomGenericData.generate(iSchema, 100, 0L).toList()
        val expectedKafkaStructs = RandomKafkaStruct.convert(iSchema, sSchema, expectedIcebergRecords).toList()

        validateKafkaReader(iSchema, sSchema, expectedIcebergRecords)
        validateKafkaWriter(iSchema, sSchema, expectedKafkaStructs)
    }

    private fun writeAndValidate(sSchema: KafkaSchema) {
        val iSchema = convert(sSchema)
        val expectedKafkaStructs = RandomKafkaStruct.generate(sSchema, 100, 5).toList()

        val expectedIcebergRecords = validateKafkaWriter(iSchema, sSchema, expectedKafkaStructs)
        validateKafkaReader(iSchema, sSchema, expectedIcebergRecords)
    }

    protected open fun convert(iSchema: IcebergSchema): KafkaSchema {
        return iSchema.toKafkaSchema()
    }

    protected open fun convert(sSchema: KafkaSchema): IcebergSchema {
        return sSchema.toIcebergSchema()
    }

    private fun validateKafkaReader(
        iSchema: IcebergSchema,
        sSchema: KafkaSchema,
        expected: List<IcebergRecord>
    ) {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }

        // Write the expected records into Parquet file, then read them into Kafka Struct and assert with the expected Record list.
        val writer: FileAppender<IcebergRecord> = Parquet.write(Files.localOutput(tmp))
            .schema(iSchema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .named("test")
            .build()

        writer.use {
            expected.forEach(it::add)
        }

        val reader: CloseableIterable<KafkaStruct> = Parquet.read(Files.localInput(tmp))
            .project(iSchema)
            .createReaderFunc { fileSchema ->
                KafkaParquetReader.buildReader(sSchema, fileSchema)
            }
            .build()
        val actual = reader.use {
            it.toList()
        }

        expected.zip(actual) { e, a ->
            assertEquals(iSchema, sSchema, e, a)
        }
    }

    private fun validateKafkaWriter(
        iSchema: IcebergSchema,
        sSchema: KafkaSchema,
        expected: List<KafkaStruct>
    ): List<IcebergRecord> {
        val tmp = temp.newFile()
        assert(tmp.delete()) { "Delete should succeed" }

        // Write the expected Kafka Struct into Parquet file, then read them into Record and assert with the expected Kafka Struct list.
        val writer: FileAppender<KafkaStruct> = Parquet.write(Files.localOutput(tmp))
            .schema(iSchema)
            .createWriterFunc { msgType ->
                KafkaParquetWriter.buildWriter(sSchema, msgType)
            }
            .named("test")
            .build()

        writer.use {
            expected.forEach(writer::add)
        }

        val reader: CloseableIterable<IcebergRecord> = Parquet.read(Files.localInput(tmp))
            .project(iSchema)
            .createReaderFunc { fileSchema ->
                GenericParquetReaders.buildReader(iSchema, fileSchema)
            }
            .build()
        val actual = reader.use {
            it.toList()
        }

        actual.zip(expected) { a, e ->
            assertEquals(iSchema, sSchema, a, e)
        }

        return actual
    }

    @Test
    fun testKafkaPrimitives() {
        val kafkaSchema = structSchema {
            KAFKA_PRIMITIVES_SCHEMA.forEach(::field)
        }
        writeAndValidate(kafkaSchema)
    }

    @Test
    fun testKafkaLogicalTypes() {
        val kafkaSchema = structSchema {
            KAFKA_LOGICAL_TYPES_SCHEMA.forEach(::field)
        }
        writeAndValidate(kafkaSchema)
    }

    @Test
    fun testDebeziumLogicalTypes() {
        val kafkaSchema = structSchema {
            DEBEZIUM_LOGICAL_TYPES_SCHEMA.forEach(::field)
        }
        writeAndValidate(kafkaSchema)
    }
}
