package dev.alluvial.sink.iceberg.parquet

import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.MILLIS
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.GroupType
import java.nio.ByteBuffer

object KafkaParquetWriters {
    fun struct(writers: List<ParquetValueWriter<*>?>, schema: GroupType): ParquetValueWriter<KafkaStruct> {
        return StructWriter(writers, schema)
    }

    fun bytes(desc: ColumnDescriptor): ParquetValueWriter<Any> {
        return BytesWriter(desc)
    }

    private class StructWriter(writers: List<ParquetValueWriter<*>?>, private val schema: GroupType) :
        ParquetValueWriters.StructWriter<KafkaStruct>(writers) {
        override fun get(struct: KafkaStruct, index: Int): Any? {
            val field = schema.fields[index]
            return struct.get(field.name)
        }
    }

    private class BytesWriter(desc: ColumnDescriptor) :
        ParquetValueWriters.PrimitiveWriter<Any>(desc) {
        override fun write(repetitionLevel: Int, data: Any) {
            when (data) {
                is ByteArray -> column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(data))
                is ByteBuffer -> column.writeBinary(repetitionLevel, Binary.fromReusedByteBuffer(data))
                else -> throw IllegalArgumentException("Unsupported write data with type ${data.javaClass}")
            }
        }
    }

    abstract class TimeWriter<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetValueWriters.PrimitiveWriter<T>(desc) {

        override fun write(repetitionLevel: Int, time: T) {
            val convertedTime = targetPrecision.floorConvert(serialize(time), sourcePrecision)
            if (targetPrecision == MILLIS)
                column.writeInteger(repetitionLevel, convertedTime.toInt()) else
                column.writeLong(repetitionLevel, convertedTime)
        }

        abstract fun serialize(time: T): Long
    }

    abstract class TimestampWriter<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : ParquetValueWriters.PrimitiveWriter<T>(desc) {
        override fun write(repetitionLevel: Int, time: T) {
            val convertedTime = targetPrecision.floorConvert(serialize(time), sourcePrecision)
            column.writeLong(repetitionLevel, convertedTime)
        }

        abstract fun serialize(ts: T): Long
    }
}
