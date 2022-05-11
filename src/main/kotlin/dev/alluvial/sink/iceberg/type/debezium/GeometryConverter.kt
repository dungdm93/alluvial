package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.avro.KafkaAvroReaders
import dev.alluvial.sink.iceberg.avro.KafkaAvroWriters
import dev.alluvial.sink.iceberg.type.AvroSchema
import dev.alluvial.sink.iceberg.type.AvroValueReader
import dev.alluvial.sink.iceberg.type.AvroValueReaders
import dev.alluvial.sink.iceberg.type.AvroValueWriter
import dev.alluvial.sink.iceberg.type.AvroValueWriters
import dev.alluvial.sink.iceberg.type.IcebergField
import dev.alluvial.sink.iceberg.type.IcebergType
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.OrcType
import dev.alluvial.sink.iceberg.type.OrcValueReader
import dev.alluvial.sink.iceberg.type.OrcValueWriter
import dev.alluvial.sink.iceberg.type.ParquetType
import dev.alluvial.sink.iceberg.type.ParquetValueReader
import dev.alluvial.sink.iceberg.type.ParquetValueWriter
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import io.debezium.data.geometry.Geometry
import org.apache.iceberg.StructLike
import org.apache.iceberg.types.Types.*
import java.util.function.Supplier

internal object GeometryConverter : LogicalTypeConverter<KafkaStruct, StructLike> {
    @Suppress("RemoveRedundantQualifierName")
    override val name = io.debezium.data.geometry.Geometry.LOGICAL_NAME

    private class GeometryStruct(struct: KafkaStruct) : StructLike {
        private val wkb = struct.getBytes(Geometry.WKB_FIELD)
        private val srid = struct.getInt32(Geometry.SRID_FIELD)

        override fun size(): Int = 2

        override fun <T> get(pos: Int, javaClass: Class<T>): T? {
            val value = when (pos) {
                0 -> wkb
                1 -> srid
                else -> throw UnsupportedOperationException("Unknown field ordinal: $pos")
            }
            return javaClass.cast(value)
        }

        override fun <T> set(pos: Int, value: T) {
            throw UnsupportedOperationException("GeometryStruct is immutable")
        }
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType = StructType.of(
        IcebergField.of(idSupplier.get(), false, Geometry.WKB_FIELD, BinaryType.get()),
        IcebergField.of(idSupplier.get(), true, Geometry.SRID_FIELD, IntegerType.get()),
    )

    override fun toIcebergValue(sValue: KafkaStruct): StructLike = GeometryStruct(sValue)

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<KafkaStruct> {
        return KafkaAvroReaders.struct(
            listOf(Geometry.WKB_FIELD, Geometry.SRID_FIELD),
            listOf(
                AvroValueReaders.bytes(),
                AvroValueReaders.union(listOf(AvroValueReaders.nulls(), AvroValueReaders.ints()))
            ),
            sSchema,
        )
    }

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<KafkaStruct> {
        return KafkaAvroWriters.struct(
            listOf(KafkaAvroWriters.bytes(), AvroValueWriters.option(0, AvroValueWriters.ints())),
            listOf(Geometry.WKB_FIELD, Geometry.SRID_FIELD)
        )
    }

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<KafkaStruct> {
        TODO("Not yet implemented")
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<KafkaStruct> {
        TODO("Not yet implemented")
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<KafkaStruct> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<KafkaStruct> {
        TODO("Not yet implemented")
    }
}
