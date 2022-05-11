package dev.alluvial.sink.iceberg.type.kafka

import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveWriterContext
import org.apache.avro.LogicalTypes
import org.apache.avro.io.Decoder
import org.apache.iceberg.avro.ValueReaders
import org.apache.iceberg.avro.ValueWriters
import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.iceberg.types.Types.DecimalType
import org.apache.kafka.connect.data.Decimal
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*
import java.math.BigDecimal
import java.math.BigInteger
import java.math.MathContext
import java.util.function.Supplier
import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.avro.ValueReader as AvroValueReader
import org.apache.iceberg.avro.ValueWriter as AvroValueWriter
import org.apache.iceberg.types.Type as IcebergType
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.orc.TypeDescription as OrcType
import org.apache.parquet.schema.Type as ParquetType

internal object DecimalConverter : LogicalTypeConverter<BigDecimal, BigDecimal> {
    @Suppress("RemoveRedundantQualifierName")
    override val name = org.apache.kafka.connect.data.Decimal.LOGICAL_NAME

    private class AvroReader(
        private val bytesReader: AvroValueReader<ByteArray>,
        precision: Int,
        private val scale: Int,
    ) : AvroValueReader<BigDecimal> {
        private val mathContext = MathContext(precision)

        override fun read(decoder: Decoder, reuse: Any?): BigDecimal {
            val bytes = bytesReader.read(decoder, null)
            return BigDecimal(BigInteger(bytes), scale, mathContext)
        }
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType {
        val params = schema.parameters()
        val precision = params.getOrDefault("connect.decimal.precision", "38").toInt()
        val scale = params.getOrDefault(Decimal.SCALE_FIELD, "10").toInt()
        return DecimalType.of(precision, scale)
    }

    override fun toIcebergValue(sValue: BigDecimal): BigDecimal = sValue

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<BigDecimal> {
        val decimal = schema.logicalType as LogicalTypes.Decimal
        return AvroReader(
            ValueReaders.decimalBytesReader(schema),
            decimal.precision, decimal.scale
        )
    }

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<BigDecimal> {
        val precision = sSchema.parameters().getOrDefault("connect.decimal.precision", "38").toInt()
        val scale = sSchema.parameters()[Decimal.SCALE_FIELD]!!.toInt()
        return ValueWriters.decimal(precision, scale)
    }

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<BigDecimal> {
        val primitive = type.asPrimitiveType()
        val decimal = primitive.logicalTypeAnnotation as DecimalLogicalTypeAnnotation
        val desc = (ctx as ParquetPrimitiveReaderContext).desc

        return when (primitive.primitiveTypeName) {
            INT32 -> ParquetValueReaders.IntegerAsDecimalReader(desc, decimal.scale)
            INT64 -> ParquetValueReaders.LongAsDecimalReader(desc, decimal.scale)
            BINARY,
            FIXED_LEN_BYTE_ARRAY -> ParquetValueReaders.BinaryAsDecimalReader(desc, decimal.scale)
            else -> throw UnsupportedOperationException(
                "Unsupported base type for decimal: ${primitive.primitiveTypeName}"
            )
        }
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<BigDecimal> {
        val primitive = type.asPrimitiveType()
        val decimal = primitive.logicalTypeAnnotation as DecimalLogicalTypeAnnotation
        val desc = (ctx as ParquetPrimitiveWriterContext).desc

        return when (primitive.primitiveTypeName) {
            INT32 -> ParquetValueWriters.decimalAsInteger(desc, decimal.precision, decimal.scale)
            INT64 -> ParquetValueWriters.decimalAsLong(desc, decimal.precision, decimal.scale)
            BINARY,
            FIXED_LEN_BYTE_ARRAY -> ParquetValueWriters.decimalAsFixed(desc, decimal.precision, decimal.scale)
            else -> throw UnsupportedOperationException(
                "Unsupported base type for decimal: ${primitive.primitiveTypeName}"
            )
        }
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<BigDecimal> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<BigDecimal> {
        TODO("Not yet implemented")
    }
}
