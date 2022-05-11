package dev.alluvial.sink.iceberg.type.kafka

import dev.alluvial.sink.iceberg.type.AvroSchema
import dev.alluvial.sink.iceberg.type.AvroValueReader
import dev.alluvial.sink.iceberg.type.AvroValueWriter
import dev.alluvial.sink.iceberg.type.IcebergType
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.OrcType
import dev.alluvial.sink.iceberg.type.OrcValueReader
import dev.alluvial.sink.iceberg.type.OrcValueWriter
import dev.alluvial.sink.iceberg.type.ParquetType
import dev.alluvial.sink.iceberg.type.ParquetValueReader
import dev.alluvial.sink.iceberg.type.ParquetValueWriter
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveWriterContext
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import org.apache.avro.io.Decoder
import org.apache.avro.io.Encoder
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.iceberg.types.Types.DateType
import org.apache.parquet.column.ColumnDescriptor
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

internal object DateConverter : LogicalTypeConverter<Date, Int> {
    override val name = org.apache.kafka.connect.data.Date.LOGICAL_NAME

    private object AvroReader : AvroValueReader<Date> {
        override fun read(decoder: Decoder, reuse: Any?): Date {
            val days = decoder.readInt().toLong()
            val time = TimeUnit.DAYS.toMillis(days)
            return if (time == (reuse as? Date)?.time) reuse else Date(time)
        }
    }

    private object AvroWriter : AvroValueWriter<Date> {
        override fun write(date: Date, encoder: Encoder) {
            val days = TimeUnit.MILLISECONDS.toDays(date.time).toInt()
            encoder.writeInt(days)
        }
    }

    private class ParquetReader(desc: ColumnDescriptor) :
        ParquetValueReaders.UnboxedReader<Date>(desc) {
        override fun read(reuse: Date?): Date {
            val days = readInteger().toLong()
            val time = TimeUnit.DAYS.toMillis(days)
            return if (time == reuse?.time) reuse else Date(time)
        }
    }

    private class ParquetWriter(desc: ColumnDescriptor) :
        ParquetValueWriters.PrimitiveWriter<Date>(desc) {
        override fun write(repetitionLevel: Int, date: Date) {
            val days = TimeUnit.MILLISECONDS.toDays(date.time).toInt()
            column.writeInteger(repetitionLevel, days)
        }
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType {
        return DateType.get()
    }

    override fun toIcebergValue(sValue: Date): Int {
        val time = sValue.time
        return TimeUnit.MILLISECONDS.toDays(time).toInt()
    }

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<Date> = AvroReader

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<Date> = AvroWriter

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Date> {
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetReader(primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Date> {
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetWriter(primitiveCtx.desc)
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<Date> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<Date> {
        TODO("Not yet implemented")
    }
}
