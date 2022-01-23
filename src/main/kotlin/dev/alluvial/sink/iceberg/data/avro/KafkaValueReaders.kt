package dev.alluvial.sink.iceberg.data.avro

import org.apache.avro.io.Decoder
import org.apache.avro.io.ResolvingDecoder
import org.apache.iceberg.avro.ValueReader
import java.math.BigDecimal
import java.math.BigInteger
import java.math.MathContext
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MICROSECONDS
import java.util.concurrent.TimeUnit.MILLISECONDS
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

    fun <E> array(elementReader: ValueReader<E>): ValueReader<List<E>> {
        return ArrayReader(elementReader)
    }

    fun <K, V> map(keyReader: ValueReader<K>, valueReader: ValueReader<V>): ValueReader<Map<K, V>> {
        return MapReader(keyReader, valueReader)
    }

    fun <K, V> arrayMap(keyReader: ValueReader<K>, valueReader: ValueReader<V>): ValueReader<Map<K, V>> {
        return ArrayMapReader(keyReader, valueReader)
    }

    fun decimal(bytesReader: ValueReader<ByteArray>, precision: Int, scale: Int): ValueReader<BigDecimal> {
        return DecimalReader(bytesReader, precision, scale)
    }

    fun date(): ValueReader<Date> {
        return DateReader
    }

    fun time(precision: TimeUnit): ValueReader<Date> {
        return when (precision) {
            MILLISECONDS -> TimeReader.MILLISECONDS
            MICROSECONDS -> TimeReader.MICROSECONDS
            else -> throw IllegalArgumentException("Precision $precision is not allowed")
        }
    }

    fun timestamp(precision: TimeUnit): ValueReader<Date> {
        return when (precision) {
            MILLISECONDS -> TimestampReader.MILLISECONDS
            MICROSECONDS -> TimestampReader.MICROSECONDS
            else -> throw IllegalArgumentException("Precision $precision is not allowed")
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

    private class DecimalReader(
        private val bytesReader: ValueReader<ByteArray>,
        precision: Int,
        private val scale: Int,
    ) : ValueReader<BigDecimal> {
        private val mathContext = MathContext(precision)

        override fun read(decoder: Decoder, reuse: Any?): BigDecimal {
            val bytes = bytesReader.read(decoder, null)
            return BigDecimal(BigInteger(bytes), scale, mathContext)
        }
    }

    private object DateReader : ValueReader<Date> {
        override fun read(decoder: Decoder, reuse: Any?): Date {
            val days = decoder.readInt().toLong()
            val time = TimeUnit.DAYS.toMillis(days)
            return if (time == (reuse as? Date)?.time) reuse else Date(time)
        }
    }

    private enum class TimeReader : ValueReader<Date> {
        MILLISECONDS {
            override fun read(decoder: Decoder, reuse: Any?): Date {
                val timeMillis = decoder.readInt().toLong()
                return Date(timeMillis)
            }
        },
        MICROSECONDS {
            override fun read(decoder: Decoder, reuse: Any?): Date {
                val timeMicros = decoder.readLong()
                var timeMillis = TimeUnit.MICROSECONDS.toMillis(timeMicros)
                // represent a time before UNIX Epoch (1970-01-01T00:00:00+GMT)
                // then, timeMicros will be negative
                if (TimeUnit.MILLISECONDS.toMicros(timeMillis) > timeMicros) {
                    timeMillis--
                }
                return if (timeMillis == (reuse as? Date)?.time) reuse else Date(timeMillis)
            }
        },
    }

    private enum class TimestampReader : ValueReader<Date> {
        MILLISECONDS {
            override fun read(decoder: Decoder, reuse: Any?): Date {
                val timeMillis = decoder.readLong()
                return Date(timeMillis)
            }
        },
        MICROSECONDS {
            override fun read(decoder: Decoder, reuse: Any?): Date {
                val timeMicros = decoder.readLong()
                var timeMillis = TimeUnit.MICROSECONDS.toMillis(timeMicros)
                // represent a timestamp before UNIX Epoch (1970-01-01T00:00:00+GMT)
                // then, timeMicros will be negative
                if (TimeUnit.MILLISECONDS.toMicros(timeMillis) > timeMicros) {
                    timeMillis--
                }
                return if (timeMillis == (reuse as? Date)?.time) reuse else Date(timeMillis)
            }
        }
    }
}
