package dev.alluvial.sink.iceberg.type

import dev.alluvial.source.kafka.fieldSchema
import org.apache.iceberg.Schema
import org.apache.iceberg.data.Record
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID.*
import org.apache.iceberg.types.Types
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Date
import java.util.concurrent.TimeUnit

/**
 * Convert Iceberg's [Record] to [KafkaStruct]
 */
internal class KafkaStructConverter(
    private val referenceSchema: KafkaSchema
) {
    private var sourceSchema: KafkaSchema? = null

    private inline fun <T> travel(schema: KafkaSchema, block: () -> T): T {
        val previousSchema = sourceSchema
        try {
            sourceSchema = schema
            return block()
        } finally {
            sourceSchema = previousSchema
        }
    }

    fun convert(iSchema: Schema, record: Record): KafkaStruct =
        travel(referenceSchema) {
            return struct(iSchema.asStruct(), record)
        }

    private fun struct(struct: Types.StructType, record: Record): KafkaStruct {
        val s = KafkaStruct(sourceSchema)

        struct.fields().forEach { iField ->
            val sField = sourceSchema!!.field(iField.name())
            val iValue = record.getField(iField.name())
            val sValue = field(iField, iValue)
            s.put(sField, sValue)
        }
        return s
    }

    private fun field(field: Types.NestedField, obj: Any?): Any? =
        travel(sourceSchema!!.fieldSchema(field.name())) {
            convert(field.type(), obj)
        }

    private fun map(map: Types.MapType, obj: Map<*, *>): Map<*, *> {
        val keyType = map.keyType()
        val valueType = map.valueType()
        val keySchema = sourceSchema!!.keySchema()
        val valueSchema = sourceSchema!!.valueSchema()

        return obj.map { (k, v) ->
            val key = travel(keySchema) { convert(keyType, k) }
            val value = travel(valueSchema) { convert(valueType, v) }
            key to value
        }.toMap()
    }

    private fun list(list: Types.ListType, obj: List<*>): List<*> =
        travel(sourceSchema!!.valueSchema()) {
            val elementType = list.elementType()
            obj.map { convert(elementType, it) }
        }

    private fun convert(type: Type, obj: Any?): Any? {
        if (obj == null) return null

        return when (type.typeId()) {
            BOOLEAN,
            INTEGER,
            LONG,
            FLOAT,
            DOUBLE,
            DECIMAL,
            STRING,
            BINARY,
            FIXED,
            UUID -> obj
            DATE -> {
                val days = (obj as LocalDate).toEpochDay()
                val time = TimeUnit.DAYS.toMillis(days)
                Date(time)
            }
            TIME -> {
                val timeNanos = (obj as LocalTime).toNanoOfDay()
                var timeMillis = TimeUnit.NANOSECONDS.toMillis(timeNanos)
                // represent a time before UNIX Epoch (1970-01-01T00:00:00+GMT)
                // then, timeMicros will be negative
                if (TimeUnit.MILLISECONDS.toNanos(timeMillis) > timeNanos) {
                    timeMillis--
                }
                Date(timeMillis)
            }
            TIMESTAMP -> {
                val timeNanos = if ((type as Types.TimestampType).shouldAdjustToUTC())
                    (obj as OffsetDateTime).toEpochSecond() else
                    (obj as LocalDateTime).toEpochSecond(ZoneOffset.UTC)
                var timeMillis = TimeUnit.NANOSECONDS.toMillis(timeNanos)
                // represent a time before UNIX Epoch (1970-01-01T00:00:00+GMT)
                // then, timeMicros will be negative
                if (TimeUnit.MILLISECONDS.toNanos(timeMillis) > timeNanos) {
                    timeMillis--
                }
                Date(timeMillis)
            }
            STRUCT -> struct(type.asStructType(), obj as Record)
            LIST -> list(type.asListType(), obj as List<*>)
            MAP -> map(type.asMapType(), obj as Map<*, *>)
            else -> throw UnsupportedOperationException("Not a supported type: $type")
        }
    }
}
