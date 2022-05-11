package dev.alluvial.sink.iceberg.parquet

import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.utils.TimePrecision
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.iceberg.parquet.ParquetValueReaders.PrimitiveReader
import org.apache.iceberg.parquet.ParquetValueReaders.UnboxedReader
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.Type

object KafkaParquetReaders {
    fun strings(desc: ColumnDescriptor): ParquetValueReader<String> {
        return ParquetValueReaders.StringReader(desc)
    }

    fun byteArrays(desc: ColumnDescriptor): ParquetValueReader<ByteArray> {
        return ParquetValueReaders.ByteArrayReader(desc)
    }

    fun <T> unboxed(desc: ColumnDescriptor): ParquetValueReader<T> {
        return UnboxedReader(desc)
    }

    fun tinyints(desc: ColumnDescriptor): ParquetValueReader<Byte> {
        return ByteReader(desc)
    }

    fun shorts(desc: ColumnDescriptor): ParquetValueReader<Short> {
        return ShortReader(desc)
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

    private class ShortReader(desc: ColumnDescriptor) : UnboxedReader<Short>(desc) {
        override fun read(ignored: Short?): Short {
            return readInteger().toShort()
        }
    }

    private class ByteReader(desc: ColumnDescriptor) : UnboxedReader<Byte>(desc) {
        override fun read(ignored: Byte?): Byte {
            return readInteger().toByte()
        }
    }

    abstract class TimeReader<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : PrimitiveReader<T>(desc) {
        override fun read(reuse: T?): T {
            var time = if (sourcePrecision == TimePrecision.MILLIS)
                column.nextInteger().toLong() else
                column.nextLong()

            time = targetPrecision.floorConvert(time, sourcePrecision)
            return deserialize(time, reuse)
        }

        abstract fun deserialize(time: Long, reuse: Any?): T
    }

    abstract class TimestampReader<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
        desc: ColumnDescriptor
    ) : PrimitiveReader<T>(desc) {
        override fun read(reuse: T?): T {
            var ts = column.nextLong()
            ts = targetPrecision.floorConvert(ts, sourcePrecision)

            return deserialize(ts, reuse)
        }

        abstract fun deserialize(ts: Long, reuse: Any?): T
    }
}
