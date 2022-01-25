package dev.alluvial.sink.iceberg.data.parquet

import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.Type
import java.util.Date
import java.util.concurrent.TimeUnit
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit as ParquetTimeUnit

object KafkaParquetReaders {
    fun strings(desc: ColumnDescriptor): ParquetValueReader<String> {
        return ParquetValueReaders.StringReader(desc)
    }

    fun byteArrays(desc: ColumnDescriptor): ParquetValueReader<ByteArray> {
        return ParquetValueReaders.ByteArrayReader(desc)
    }

    fun <T> unboxed(desc: ColumnDescriptor): ParquetValueReader<T> {
        return ParquetValueReaders.UnboxedReader(desc)
    }

    fun list(
        definitionLevel: Int,
        repetitionLevel: Int,
        elementReader: ParquetValueReader<*>
    ): ParquetValueReader<out List<*>> {
        return ParquetValueReaders.ListReader(definitionLevel, repetitionLevel, elementReader)
    }

    fun map(
        definitionLevel: Int,
        repetitionLevel: Int,
        keyWriter: ParquetValueReader<*>,
        valueWriter: ParquetValueReader<*>
    ): ParquetValueReader<out Map<*, *>> {
        return ParquetValueReaders.MapReader(definitionLevel, repetitionLevel, keyWriter, valueWriter)
    }

    fun date(desc: ColumnDescriptor): ParquetValueReader<Date> {
        return DateReader(desc)
    }

    fun time(desc: ColumnDescriptor, logicalType: TimeLogicalTypeAnnotation): ParquetValueReader<Date> {
        return TimeReader(desc, logicalType.unit)
    }

    fun timestamp(desc: ColumnDescriptor, logicalType: TimestampLogicalTypeAnnotation): ParquetValueReader<Date> {
        return TimestampReader(desc, logicalType.unit)
    }

    fun struct(
        types: List<Type>,
        fieldReaders: List<ParquetValueReader<*>?>,
        struct: KafkaSchema,
    ): ParquetValueReader<*> {
        return StructReader(types, fieldReaders, struct)
    }

    private class StructReader(
        types: List<Type>,
        readers: List<ParquetValueReader<*>?>,
        private val kafkaSchema: KafkaSchema,
    ) : ParquetValueReaders.StructReader<KafkaStruct, KafkaStruct>(types, readers) {
        override fun newStructData(reuse: KafkaStruct?): KafkaStruct {
            return if (reuse is KafkaStruct)
                reuse else
                KafkaStruct(kafkaSchema)
        }

        override fun buildStruct(struct: KafkaStruct): KafkaStruct = struct

        override fun getField(intermediate: KafkaStruct, pos: Int): Any? {
            val field = kafkaSchema.fields()[pos]
            return intermediate.get(field)
        }

        override fun set(struct: KafkaStruct, pos: Int, value: Any?) {
            val field = kafkaSchema.fields()[pos]
            struct.put(field, value)
        }
    }

    private val TIME_UNIT_MAP = mapOf(
        ParquetTimeUnit.MILLIS to TimeUnit.MILLISECONDS,
        ParquetTimeUnit.MICROS to TimeUnit.MICROSECONDS,
        ParquetTimeUnit.NANOS to TimeUnit.NANOSECONDS,
    )

    private class DateReader(desc: ColumnDescriptor) : ParquetValueReaders.UnboxedReader<Date>(desc) {
        override fun read(reuse: Date?): Date {
            val days = readInteger().toLong()
            val time = TimeUnit.DAYS.toMillis(days)
            return if (time == reuse?.time) reuse else Date(time)
        }
    }

    private class TimeReader(desc: ColumnDescriptor, timeUnit: ParquetTimeUnit) :
        ParquetValueReaders.UnboxedReader<Date>(desc) {
        private val sourceUnit = TIME_UNIT_MAP[timeUnit]!!

        override fun read(reuse: Date?): Date {
            val timeOriginal = if (sourceUnit == TimeUnit.MILLISECONDS)
                readInteger().toLong() else
                readLong()
            val timeMillis = TimeUnit.MILLISECONDS.convert(timeOriginal, sourceUnit)
            return if (timeMillis == reuse?.time) reuse else Date(timeMillis)
        }
    }

    private class TimestampReader(desc: ColumnDescriptor, timeUnit: ParquetTimeUnit) :
        ParquetValueReaders.UnboxedReader<Date>(desc) {
        private val sourceUnit = TIME_UNIT_MAP[timeUnit]!!

        override fun read(reuse: Date?): Date {
            val timeOriginal = readLong()
            var timeMillis = TimeUnit.MILLISECONDS.convert(timeOriginal, sourceUnit)
            // represent a time before UNIX Epoch (1970-01-01T00:00:00+GMT)
            // then, timeMicros will be negative
            if (sourceUnit.convert(timeMillis, TimeUnit.MILLISECONDS) > timeOriginal) {
                timeMillis--
            }
            return if (timeMillis == reuse?.time) reuse else Date(timeMillis)
        }
    }
}
