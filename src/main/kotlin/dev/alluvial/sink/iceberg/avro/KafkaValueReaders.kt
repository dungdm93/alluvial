package dev.alluvial.sink.iceberg.avro

import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.MILLIS
import dev.alluvial.utils.TimePrecision.NANOS
import org.apache.avro.io.Decoder
import org.apache.avro.io.ResolvingDecoder
import org.apache.iceberg.avro.ValueReader
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

object KafkaValueReaders {
    fun struct(
        names: List<String>,
        fieldReaders: List<ValueReader<*>>,
        struct: KafkaSchema,
    ): ValueReader<KafkaStruct> {
        return StructReader(names, fieldReaders, struct)
    }

    fun <N : Number> bytes(delegator: ValueReader<N>): ValueReader<Byte> {
        return ByteReader(delegator)
    }

    fun <N : Number> shorts(delegator: ValueReader<N>): ValueReader<Short> {
        return ShortReader(delegator)
    }

    fun <E> array(elementReader: ValueReader<E>): ValueReader<List<E>> {
        return ArrayReader(elementReader)
    }

    fun <K, V> map(keyReader: ValueReader<K>, valueReader: ValueReader<V>): ValueReader<Map<K, V>> {
        return MapReader(keyReader, valueReader)
    }

    fun <K, V> arrayMap(keyReader: ValueReader<K>, valueReader: ValueReader<V>): ValueReader<Map<K, V>> {
        return ArrayMapReader(keyReader, valueReader)
    }

    private class ByteReader<N : Number>(private val delegator: ValueReader<N>) : ValueReader<Byte> {
        override fun read(decoder: Decoder, reuse: Any?): Byte {
            val number = delegator.read(decoder, reuse)
            return number.toByte()
        }
    }

    private class ShortReader<N : Number>(private val delegator: ValueReader<N>) : ValueReader<Short> {
        override fun read(decoder: Decoder, reuse: Any?): Short {
            val number = delegator.read(decoder, reuse)
            return number.toShort()
        }
    }

    /**
     * @see org.apache.iceberg.avro.ValueReaders.StructReader
     */
    private class StructReader(
        private val names: List<String>,
        private val fieldReaders: List<ValueReader<*>>,
        private val kafkaSchema: KafkaSchema,
    ) : ValueReader<KafkaStruct> {
        override fun read(decoder: Decoder, reuse: Any?): KafkaStruct {
            val struct = reuseOrCreate(reuse)

            if (decoder is ResolvingDecoder) {
                // this may not set all the fields. nulls are set by default.
                for (field in decoder.readFieldOrder()) {
                    val pos = field.pos()
                    val reusedValue = get(struct, pos)
                    val value = fieldReaders[pos].read(decoder, reusedValue)
                    set(struct, pos, value)
                }
            } else {
                fieldReaders.forEachIndexed { i, reader ->
                    val reusedValue = get(struct, i)
                    set(struct, i, reader.read(decoder, reusedValue))
                }
            }

//            for (int i = 0; i < positions.length; i += 1) {
//                set(struct, positions[i], constants[i]);
//            }

            return struct
        }

        fun reuseOrCreate(reuse: Any?): KafkaStruct {
            return if (reuse is KafkaStruct)
                reuse else
                KafkaStruct(kafkaSchema)
        }

        fun get(struct: KafkaStruct, pos: Int): Any? {
            val field = names[pos]
            return struct.get(field)
        }

        fun set(struct: KafkaStruct, pos: Int, value: Any?) {
            val field = names[pos]
            struct.put(field, value)
        }
    }

    private class ArrayReader<E>(
        private val elementReader: ValueReader<E>
    ) : ValueReader<List<E>> {
        private val reused = mutableListOf<E>()

        override fun read(decoder: Decoder, reuse: Any?): List<E> {
            reused.clear()

            var chunkLength = decoder.readArrayStart()
            while (chunkLength > 0) {
                for (idx in 0 until chunkLength) {
                    val element = elementReader.read(decoder, null)
                    reused.add(element)
                }
                chunkLength = decoder.arrayNext()
            }

            return reused.toList()
        }
    }

    private class MapReader<K, V>(
        private val keyReader: ValueReader<K>,
        private val valueReader: ValueReader<V>
    ) : ValueReader<Map<K, V>> {
        private val reusedKey = mutableListOf<K>()
        private val reusedValue = mutableListOf<V>()

        override fun read(decoder: Decoder, reuse: Any?): Map<K, V> {
            reusedKey.clear()
            reusedValue.clear()

            var chunkLength = decoder.readMapStart()
            while (chunkLength > 0) {
                for (idx in 0 until chunkLength) {
                    val key = keyReader.read(decoder, null)
                    val value = valueReader.read(decoder, null)
                    reusedKey.add(key)
                    reusedValue.add(value)
                }
                chunkLength = decoder.mapNext()
            }

            return reusedKey.zip(reusedValue).toMap()
        }
    }

    private class ArrayMapReader<K, V>(
        private val keyReader: ValueReader<K>,
        private val valueReader: ValueReader<V>
    ) : ValueReader<Map<K, V>> {
        private val reusedKey = mutableListOf<K>()
        private val reusedValue = mutableListOf<V>()

        override fun read(decoder: Decoder, reuse: Any?): Map<K, V> {
            reusedKey.clear()
            reusedValue.clear()

            var chunkLength = decoder.readArrayStart()
            while (chunkLength > 0) {
                for (idx in 0 until chunkLength) {
                    val key = keyReader.read(decoder, null)
                    val value = valueReader.read(decoder, null)
                    reusedKey.add(key)
                    reusedValue.add(value)
                }
                chunkLength = decoder.arrayNext()
            }

            return reusedKey.zip(reusedValue).toMap()
        }
    }

    abstract class TimeReader<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
    ) : ValueReader<T> {
        init {
            if (sourcePrecision == NANOS) {
                throw IllegalArgumentException("Avro has no $sourcePrecision precision time")
            }
        }

        override fun read(decoder: Decoder, reuse: Any?): T {
            var time = if (sourcePrecision == MILLIS)
                decoder.readInt().toLong() else
                decoder.readLong()

            time = targetPrecision.floorConvert(time, sourcePrecision)
            return deserialize(time, reuse)
        }

        abstract fun deserialize(time: Long, reuse: Any?): T
    }

    abstract class TimestampReader<T>(
        protected val sourcePrecision: TimePrecision,
        protected val targetPrecision: TimePrecision,
    ) : ValueReader<T> {
        init {
            if (sourcePrecision == NANOS) {
                throw IllegalArgumentException("Avro has no $sourcePrecision precision timestamp")
            }
        }

        override fun read(decoder: Decoder, reuse: Any?): T {
            var ts = decoder.readLong()
            ts = targetPrecision.floorConvert(ts, sourcePrecision)
            return deserialize(ts, reuse)
        }

        abstract fun deserialize(ts: Long, reuse: Any?): T
    }
}
