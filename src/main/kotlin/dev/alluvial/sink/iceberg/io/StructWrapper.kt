package dev.alluvial.sink.iceberg.io

import dev.alluvial.utils.OffsetTimes
import dev.alluvial.utils.TimePrecision.*
import dev.alluvial.utils.ZonedDateTimes
import org.apache.iceberg.StructLike
import org.apache.iceberg.types.Type.TypeID.*
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.TimestampType
import java.time.ZoneOffset
import org.apache.kafka.connect.data.Schema as KafkaSchema
import java.time.ZonedDateTime
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct
import java.util.Date as JDate
import org.apache.iceberg.util.DateTimeUtil
import java.nio.ByteBuffer
import java.time.OffsetTime
import java.util.concurrent.TimeUnit


class StructWrapper(iStruct: Types.StructType) : StructLike {
    private val getters: List<Getter<*>> = iStruct.fields().map(::getter)
    private var wrapped: KafkaStruct? = null

    constructor(iSchema: IcebergSchema) : this(iSchema.asStruct())

    fun wrap(data: KafkaStruct): StructWrapper {
        this.wrapped = data
        return this
    }

    override fun size(): Int {
        return getters.size
    }

    override fun <T> get(pos: Int, javaClass: Class<T>): T? {
        val getter = getters[pos]
        val value = getter(wrapped!!)
        return javaClass.cast(value)
    }

    override fun <T> set(pos: Int, value: T?) {
        throw UnsupportedOperationException("StructWrapper is read-only")
    }

    private fun getter(field: Types.NestedField): Getter<*> {
        val fieldName = field.name()
        val fieldType = field.type()
        return when (fieldType.typeId()) {
            INTEGER -> intGetter(fieldName)
            DATE -> dateGetter(fieldName)
            TIME -> timeGetter(fieldName)
            TIMESTAMP -> if ((fieldType as TimestampType).shouldAdjustToUTC()) timestampTzGetter(fieldName)
            else timestampGetter(fieldName)
            BINARY -> binaryGetter(fieldName)
            STRUCT -> {
                val nestedWrapper = StructWrapper(fieldType.asStructType())
                return { struct ->
                    val nestedStruct = struct[fieldName]
                    nestedStruct?.let { nestedWrapper.wrap(it as KafkaStruct) }
                }
            }
//            LIST ->
//            MAP ->
            else -> { struct -> struct[fieldName] }
        }
    }

    private fun intGetter(fieldName: String): Getter<Int> {
        return getter@{ struct ->
            val value = struct[fieldName] ?: return@getter null
            val schema = struct.schema().field(fieldName).schema()

            when (schema.type()) {
                KafkaSchema.Type.INT8 -> (value as Byte).toInt()
                KafkaSchema.Type.INT16 -> (value as Short).toInt()
                else -> value as Int
            }
        }
    }

    private fun dateGetter(fieldName: String): Getter<Int> {
        return getter@{ struct ->
            val value = struct[fieldName] ?: return@getter null
            val schema = struct.fieldSchema(fieldName)

            when (schema.name()) {
                io.debezium.time.Date.SCHEMA_NAME -> value as Int
                org.apache.kafka.connect.data.Date.LOGICAL_NAME ->
                    TimeUnit.MILLISECONDS.toDays((value as JDate).time).toInt()
                else -> throw UnsupportedOperationException("Wrapper does not support type ${schema.name()}")
            }
        }
    }

    private fun timeGetter(fieldName: String): Getter<Long> {
        return getter@{ struct ->
            val value = struct[fieldName] ?: return@getter null
            val schema = struct.fieldSchema(fieldName)

            when (schema.name()) {
                // Debezium types
                io.debezium.time.Time.SCHEMA_NAME -> MICROS.convert((value as Int).toLong(), MILLIS)
                io.debezium.time.MicroTime.SCHEMA_NAME -> (value as Long)
                io.debezium.time.NanoTime.SCHEMA_NAME -> MICROS.convert((value as Long), NANOS)
                io.debezium.time.ZonedTime.SCHEMA_NAME -> {
                    val ot = OffsetTime.parse(value as String)
                    OffsetTimes.toUtcMidnightTime(ot, MICROS)
                }
                org.apache.kafka.connect.data.Time.LOGICAL_NAME ->
                    DateTimeUtil.microsFromInstant((value as JDate).toInstant())
                else -> throw UnsupportedOperationException("Wrapper does not support type ${schema.name()}")
            }
        }
    }

    private fun timestampGetter(fieldName: String): Getter<Long> {
        return getter@{ struct ->
            val value = struct[fieldName] ?: return@getter null
            val schema = struct.fieldSchema(fieldName)

            when (schema.name()) {
                io.debezium.time.Timestamp.SCHEMA_NAME -> MICROS.convert(value as Long, MILLIS)
                io.debezium.time.MicroTimestamp.SCHEMA_NAME -> value as Long
                io.debezium.time.NanoTimestamp.SCHEMA_NAME -> MICROS.convert(value as Long, NANOS)
                org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME -> MICROS.convert((value as JDate).time, MILLIS)
                else -> throw UnsupportedOperationException("Wrapper does not support type ${schema.name()}")
            }
        }
    }

    private fun timestampTzGetter(fieldName: String): Getter<Long> {
        return getter@{ struct ->
            val value = struct[fieldName] ?: return@getter null
            val zdt = ZonedDateTime.parse(value as String)
            ZonedDateTimes.toEpochTime(zdt, MICROS)
        }
    }

    private fun binaryGetter(fieldName: String): Getter<*> {
        return getter@{ struct ->
            val value = struct[fieldName] ?: return@getter null
            when (value) {
                is ByteArray -> ByteBuffer.wrap(value)
                is ByteBuffer -> value
                else -> throw UnsupportedOperationException(
                    "Wrapper does not support binary type ${value.javaClass.name}"
                )
            }
        }
    }

    private fun KafkaStruct.fieldSchema(name: String): KafkaSchema {
        return this.schema().field(name).schema()
    }
}
