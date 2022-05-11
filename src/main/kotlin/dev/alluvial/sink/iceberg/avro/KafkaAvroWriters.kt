package dev.alluvial.sink.iceberg.avro

import dev.alluvial.sink.iceberg.type.AvroValueWriter
import dev.alluvial.sink.iceberg.type.AvroValueWriters
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.MILLIS
import dev.alluvial.utils.TimePrecision.NANOS
import org.apache.avro.io.Encoder
import java.nio.ByteBuffer

object KafkaAvroWriters {
    fun struct(writers: List<AvroValueWriter<*>>, fields: List<String>): AvroValueWriter<KafkaStruct> {
        return StructWriter(writers, fields)
    }

    fun <E> array(elementWriter: AvroValueWriter<E>): AvroValueWriter<List<E>> {
        return ArrayWriter(elementWriter)
    }

    fun <K, V> map(keyWriter: AvroValueWriter<K>, valueWriter: AvroValueWriter<V>): AvroValueWriter<Map<K, V>> {
        return MapWriter(keyWriter, valueWriter)
    }

    fun <K, V> arrayMap(keyWriter: AvroValueWriter<K>, valueWriter: AvroValueWriter<V>): AvroValueWriter<Map<K, V>> {
        return ArrayMapWriter(keyWriter, valueWriter)
    }

    fun bytes(): AvroValueWriter<*> {
        return BytesWriter
    }

    private class StructWriter(writers: List<AvroValueWriter<*>>, fields: List<String>) : AvroValueWriter<KafkaStruct> {
        private val writers: List<Pair<String, AvroValueWriter<*>>>

        init {
            assert(writers.size == fields.size) { "writers.size and fields.size must be equal" }
            this.writers = fields.zip(writers)
        }

        override fun write(struct: KafkaStruct, encoder: Encoder) {
            writers.forEach { (field, writer) ->
                writeField(struct, field, writer, encoder)
            }
        }

        private fun <T> writeField(struct: KafkaStruct, field: String, writer: AvroValueWriter<T>, encoder: Encoder) {
            @Suppress("UNCHECKED_CAST")
            writer.write(struct.get(field) as T, encoder)
        }
    }

    private class ArrayWriter<E>(
        private val elementWriter: AvroValueWriter<E>
    ) : AvroValueWriter<List<E>> {
        override fun write(array: List<E>, encoder: Encoder) {
            encoder.writeArrayStart()
            val numElements: Int = array.size
            encoder.setItemCount(numElements.toLong())
            array.forEach {
                encoder.startItem()
                elementWriter.write(it, encoder)
            }
            encoder.writeArrayEnd()
        }
    }

    private class MapWriter<K, V>(
        private val keyWriter: AvroValueWriter<K>,
        private val valueWriter: AvroValueWriter<V>,
    ) : AvroValueWriter<Map<K, V>> {
        override fun write(map: Map<K, V>, encoder: Encoder) {
            encoder.writeMapStart()
            val numElements: Int = map.size
            encoder.setItemCount(numElements.toLong())
            map.forEach { (k, v) ->
                encoder.startItem()
                keyWriter.write(k, encoder)
                valueWriter.write(v, encoder)
            }
            encoder.writeMapEnd()
        }
    }

    private class ArrayMapWriter<K, V>(
        private val keyWriter: AvroValueWriter<K>,
        private val valueWriter: AvroValueWriter<V>,
    ) : AvroValueWriter<Map<K, V>> {
        override fun write(map: Map<K, V>, encoder: Encoder) {
            encoder.writeArrayStart()
            val numElements: Int = map.size
            encoder.setItemCount(numElements.toLong())
            map.forEach { (k, v) ->
                encoder.startItem()
                keyWriter.write(k, encoder)
                valueWriter.write(v, encoder)
            }
            encoder.writeArrayEnd()
        }
    }

    abstract class TimeWriter<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
    ) : AvroValueWriter<T> {
        init {
            if (targetPrecision == NANOS) {
                throw IllegalArgumentException("Avro has no $targetPrecision precision time")
            }
        }

        override fun write(datum: T, encoder: Encoder) {
            var time = serialize(datum)
            time = targetPrecision.floorConvert(time, sourcePrecision)
            if (targetPrecision == MILLIS)
                encoder.writeInt(time.toInt()) else
                encoder.writeLong(time)
        }

        abstract fun serialize(time: T): Long
    }

    abstract class TimestampWriter<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
    ) : AvroValueWriter<T> {
        init {
            if (targetPrecision == NANOS) {
                throw IllegalArgumentException("Avro has no $targetPrecision precision timestamp")
            }
        }

        override fun write(ts: T, encoder: Encoder) {
            var time = serialize(ts)
            time = targetPrecision.floorConvert(time, sourcePrecision)
            encoder.writeLong(time)
        }

        abstract fun serialize(ts: T): Long
    }

    private object BytesWriter : AvroValueWriter<Any> {
        private val byteArrayWriter = AvroValueWriters.bytes()
        private val byteBufferWriter = AvroValueWriters.byteBuffers()

        override fun write(datum: Any, encoder: Encoder) {
            when (datum) {
                is ByteArray -> byteArrayWriter.write(datum, encoder)
                is ByteBuffer -> byteBufferWriter.write(datum, encoder)
                else -> throw IllegalArgumentException("Unsupported write data with type ${datum.javaClass}")
            }
        }
    }
}
