package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.type.AvroSchema
import dev.alluvial.sink.iceberg.type.AvroValueReader
import dev.alluvial.sink.iceberg.type.AvroValueWriter
import dev.alluvial.sink.iceberg.type.IcebergType
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.OrcType
import dev.alluvial.sink.iceberg.type.OrcValueReader
import dev.alluvial.sink.iceberg.type.OrcValueWriter
import dev.alluvial.sink.iceberg.type.ParquetType
import dev.alluvial.sink.iceberg.type.ParquetValueReader
import dev.alluvial.sink.iceberg.type.ParquetValueWriter
import dev.alluvial.sink.iceberg.type.kafka.DecimalConverter
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import io.debezium.data.VariableScaleDecimal
import io.debezium.data.VariableScaleDecimal.SCALE_FIELD
import io.debezium.data.VariableScaleDecimal.VALUE_FIELD
import org.apache.avro.io.Decoder
import org.apache.avro.io.Encoder
import org.apache.iceberg.parquet.DelegatedParquetReader
import org.apache.iceberg.parquet.DelegatedParquetWriter
import org.apache.iceberg.types.Types.DecimalType
import org.apache.kafka.connect.data.Struct
import java.math.BigDecimal
import java.math.BigInteger
import java.math.MathContext
import java.util.function.Supplier

object VariableScaleDecimalConverter : LogicalTypeConverter<KafkaStruct, BigDecimal> {
    override val name = VariableScaleDecimal.LOGICAL_NAME

    private val DECIMAL_TYPE = DecimalType.of(38, 18)
    private val mathContext = MathContext(DECIMAL_TYPE.precision())

    private class AvroReader(
        private val decimalReader: AvroValueReader<BigDecimal>,
        private val sSchema: KafkaSchema
    ) : AvroValueReader<KafkaStruct> {
        override fun read(decoder: Decoder, reuse: Any?): KafkaStruct {
            val decimal = decimalReader.read(decoder, null)
            return fromIcebergValue(decimal, sSchema, reuse)
        }
    }

    private class AvroWriter(
        private val decimalWriter: AvroValueWriter<BigDecimal>
    ) : AvroValueWriter<KafkaStruct> {
        override fun write(struct: KafkaStruct, encoder: Encoder) {
            val decimal = toIcebergValue(struct)
            decimalWriter.write(decimal, encoder)
        }
    }

    private class ParquetReader(
        decimalReader: ParquetValueReader<BigDecimal>,
        private val sSchema: KafkaSchema,
    ) : DelegatedParquetReader<KafkaStruct, BigDecimal>(decimalReader) {
        override fun read(reuse: KafkaStruct?): KafkaStruct {
            val decimal = delegator.read(null)
            return fromIcebergValue(decimal, sSchema, reuse)
        }
    }

    private class ParquetWriter(
        decimalWriter: ParquetValueWriter<BigDecimal>,
    ) : DelegatedParquetWriter<KafkaStruct, BigDecimal>(decimalWriter) {
        override fun write(repetitionLevel: Int, struct: KafkaStruct) {
            val decimal = toIcebergValue(struct)
            delegator.write(repetitionLevel, decimal)
        }
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType = DECIMAL_TYPE

    override fun toIcebergValue(sValue: Struct): BigDecimal {
        val bytes = sValue.getBytes(VALUE_FIELD)
        val scale = sValue.getInt32(SCALE_FIELD)

        return BigDecimal(BigInteger(bytes), scale, mathContext)
            .setScale(DECIMAL_TYPE.scale(), mathContext.roundingMode)
    }

    private fun fromIcebergValue(iValue: BigDecimal, sSchema: KafkaSchema, reuse: Any?): KafkaStruct {
        val sStruct = if (reuse is KafkaStruct)
            reuse else
            KafkaStruct(sSchema)
        sStruct.put(SCALE_FIELD, iValue.scale())
        sStruct.put(VALUE_FIELD, iValue.unscaledValue().toByteArray())
        return sStruct
    }

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<KafkaStruct> {
        val decimalReader = DecimalConverter.avroReader(sSchema, schema)
        return AvroReader(decimalReader, sSchema)
    }

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<KafkaStruct> {
        val decimalWriter = DecimalConverter.avroWriter(sSchema, schema)
        return AvroWriter(decimalWriter)
    }

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<KafkaStruct> {
        val decimalReader = DecimalConverter.parquetReader(sSchema, type, ctx)
        return ParquetReader(decimalReader, sSchema)
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<KafkaStruct> {
        val decimalWriter = DecimalConverter.parquetWriter(sSchema, type, ctx)
        return ParquetWriter(decimalWriter)
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<KafkaStruct> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<KafkaStruct> {
        TODO("Not yet implemented")
    }
}
