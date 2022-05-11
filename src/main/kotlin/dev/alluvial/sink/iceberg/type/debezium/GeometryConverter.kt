package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.avro.KafkaValueReaders
import dev.alluvial.sink.iceberg.avro.KafkaValueWriters
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import io.debezium.data.geometry.Geometry
import org.apache.iceberg.StructLike
import org.apache.iceberg.avro.ValueReader
import org.apache.iceberg.avro.ValueReaders
import org.apache.iceberg.avro.ValueWriter
import org.apache.iceberg.avro.ValueWriters
import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types.*
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.orc.TypeDescription
import java.util.function.Supplier

internal object GeometryConverter : LogicalTypeConverter<Struct, StructLike> {
    @Suppress("RemoveRedundantQualifierName")
    override val name = io.debezium.data.geometry.Geometry.LOGICAL_NAME

    private class GeometryStruct(struct: Struct) : StructLike {
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

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type = StructType.of(
        NestedField.of(idSupplier.get(), false, Geometry.WKB_FIELD, BinaryType.get()),
        NestedField.of(idSupplier.get(), true, Geometry.SRID_FIELD, IntegerType.get()),
    )

    override fun toIcebergValue(sValue: Struct): StructLike = GeometryStruct(sValue)

    override fun avroReader(sSchema: Schema, schema: org.apache.avro.Schema): ValueReader<Struct> {
        return KafkaValueReaders.struct(
            listOf(Geometry.WKB_FIELD, Geometry.SRID_FIELD),
            listOf(ValueReaders.bytes(), ValueReaders.union(listOf(ValueReaders.nulls(), ValueReaders.ints()))),
            sSchema,
        )
    }

    override fun avroWriter(sSchema: Schema, schema: org.apache.avro.Schema): ValueWriter<Struct> {
        return KafkaValueWriters.struct(
            listOf(KafkaValueWriters.bytes(), ValueWriters.option(0, ValueWriters.ints())),
            listOf(Geometry.WKB_FIELD, Geometry.SRID_FIELD)
        )
    }

    override fun parquetReader(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Struct> {
        TODO("Not yet implemented")
    }

    override fun parquetWriter(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Struct> {
        TODO("Not yet implemented")
    }

    override fun orcReader(sSchema: Schema, type: TypeDescription): OrcValueReader<Struct> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: Schema, type: TypeDescription): OrcValueWriter<Struct> {
        TODO("Not yet implemented")
    }
}
