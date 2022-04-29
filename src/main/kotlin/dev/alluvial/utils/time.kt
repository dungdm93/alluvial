package dev.alluvial.utils

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import dev.alluvial.utils.TimePrecision.*
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.util.regex.Pattern

enum class TimePrecision(
    val scale: Long
) {
    NANOS(1L),
    MICROS(1_000L),
    MILLIS(1_000_000L);

    val ofSecond = 1_000_000_000L / scale

    fun convert(time: Long, sourcePrecision: TimePrecision): Long {
        if (this == sourcePrecision) return time
        return time * sourcePrecision.scale / this.scale
    }

    fun floorConvert(time: Long, sourcePrecision: TimePrecision): Long {
        if (this == sourcePrecision) return time
        return if (this.scale > sourcePrecision.scale)
            Math.floorDiv(time, this.scale / sourcePrecision.scale) else
            Math.multiplyExact(time, sourcePrecision.scale / this.scale)
    }
}

fun LogicalType.timePrecision(): TimePrecision {
    return when (this) {
        is LogicalTypes.TimeMillis,
        is LogicalTypes.TimestampMillis,
        is LogicalTypes.LocalTimestampMillis -> MILLIS
        is LogicalTypes.TimeMicros,
        is LogicalTypes.TimestampMicros,
        is LogicalTypes.LocalTimestampMicros -> MICROS
        else -> throw IllegalArgumentException("Unknown time precision of logicalType: ${this.name}")
    }
}

fun LogicalTypeAnnotation.timePrecision(): TimePrecision {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (this) {
        is TimeLogicalTypeAnnotation -> when (unit) {
            LogicalTypeAnnotation.TimeUnit.MILLIS -> MILLIS
            LogicalTypeAnnotation.TimeUnit.MICROS -> MICROS
            LogicalTypeAnnotation.TimeUnit.NANOS -> NANOS
        }
        is TimestampLogicalTypeAnnotation -> when (unit) {
            LogicalTypeAnnotation.TimeUnit.MILLIS -> MILLIS
            LogicalTypeAnnotation.TimeUnit.MICROS -> MICROS
            LogicalTypeAnnotation.TimeUnit.NANOS -> NANOS
        }
        else -> throw IllegalArgumentException("This must be time/timestamp type annotation")
    }
}

// java.time.LocalDate.ofEpochDay
// java.time.LocalDate.toEpochDay

object LocalTimes {
    fun ofMidnightTime(time: Long, precision: TimePrecision = NANOS): LocalTime {
        return LocalTime.ofNanoOfDay(time * precision.scale)
    }

    fun toMidnightTime(localTime: LocalTime, precision: TimePrecision = NANOS): Long {
        return localTime.toNanoOfDay() / precision.scale
    }
}

object OffsetTimes {
    fun ofUtcMidnightTime(time: Long, precision: TimePrecision = NANOS, tz: ZoneOffset = UTC): OffsetTime {
        val nod = time * precision.scale + tz.totalSeconds * precision.ofSecond
        val localTime = LocalTime.ofNanoOfDay(nod)
        return OffsetTime.of(localTime, tz)
    }

    fun toUtcMidnightTime(offsetTime: OffsetTime, precision: TimePrecision = NANOS): Long {
        val nod = offsetTime.toLocalTime().toNanoOfDay()
        val offsetNanos = offsetTime.offset.totalSeconds * precision.ofSecond
        return (nod - offsetNanos) / precision.scale
    }
}

object LocalDateTimes {
    fun ofLocalEpochTime(time: Long, precision: TimePrecision = NANOS): LocalDateTime {
        val epochSecond = Math.floorDiv(time, precision.ofSecond)
        val nanoOfSecond = Math.floorMod(time, precision.ofSecond).toInt()
        return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, UTC)
    }

    fun toLocalEpochTime(localDateTime: LocalDateTime, precision: TimePrecision = NANOS): Long {
        val epochSecond = localDateTime.toEpochSecond(UTC)
        val nanoOfSecond = localDateTime.nano
        return Math.multiplyExact(epochSecond, precision.ofSecond) + nanoOfSecond / precision.scale
    }
}

object OffsetDateTimes {
    fun ofEpochTime(time: Long, precision: TimePrecision = NANOS, tz: ZoneOffset = UTC): OffsetDateTime {
        val epochSecond = Math.floorDiv(time, precision.ofSecond)
        val nanoOfSecond = (Math.floorMod(time, precision.ofSecond) * precision.scale).toInt()
        val localDateTime = LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, tz)
        return OffsetDateTime.of(localDateTime, tz)
    }

    fun toEpochTime(offsetDateTime: OffsetDateTime, precision: TimePrecision = NANOS): Long {
        val epochSecond = offsetDateTime.toEpochSecond()
        val nanoOfSecond = offsetDateTime.nano
        return Math.multiplyExact(epochSecond, precision.ofSecond) + nanoOfSecond / precision.scale
    }
}

object ZonedDateTimes {
    fun ofEpochTime(time: Long, precision: TimePrecision = NANOS, tz: ZoneOffset = UTC): ZonedDateTime {
        val epochSecond = Math.floorDiv(time, precision.ofSecond)
        val nanoOfSecond = (Math.floorMod(time, precision.ofSecond) * precision.scale).toInt()
        val localDateTime = LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, tz)
        return ZonedDateTime.of(localDateTime, tz)
    }

    fun toEpochTime(zonedDateTime: ZonedDateTime, precision: TimePrecision = NANOS): Long {
        val epochSecond = zonedDateTime.toEpochSecond()
        val nanoOfSecond = zonedDateTime.nano
        return Math.multiplyExact(epochSecond, precision.ofSecond) + nanoOfSecond / precision.scale
    }
}

object Instants {
    fun ofEpochTime(time: Long, precision: TimePrecision = NANOS): Instant {
        val epochSecond = Math.floorDiv(time, precision.ofSecond)
        val nanoOfSecond = Math.floorMod(time, precision.ofSecond) * precision.scale
        return Instant.ofEpochSecond(epochSecond, nanoOfSecond)
    }

    fun toEpochTime(instant: Instant, precision: TimePrecision = NANOS): Long {
        val epochSecond = instant.epochSecond
        val nanoOfSecond = instant.nano
        return Math.multiplyExact(epochSecond, precision.ofSecond) + nanoOfSecond / precision.scale
    }
}

private val pattern: Pattern = Pattern.compile(
    """\s*(?:(?<hours>\d+)\s*(?:hours?|hrs?|h))?""" +
        """\s*(?:(?<minutes>\d+)\s*(?:minutes?|mins?|m))?""" +
        """\s*(?:(?<seconds>\d+)\s*(?:seconds?|secs?|s))?""" +
        """\s*(?:(?<millis>\d+)\s*(?:milliseconds?|millis?|ms))?""" +
        """\s*(?:(?<micros>\d+)\s*(?:microseconds?|micros?|us|µs))?""" +
        """\s*(?:(?<nanos>\d+)\s*(?:nanoseconds?|nanos?|ns))?""" +
        """\s*""", Pattern.CASE_INSENSITIVE
)

fun String.toDuration(): Duration {
    val m = pattern.matcher(this)
    if (!m.matches()) throw IllegalArgumentException("Not valid duration: $this")

    val hours = m.group("hours")?.toInt() ?: 0
    val mins = m.group("minutes")?.toInt() ?: 0
    val secs = m.group("seconds")?.toInt() ?: 0

    val millis = m.group("millis")?.toInt() ?: 0
    val micros = m.group("micros")?.toInt() ?: 0
    val nanos = m.group("nanos")?.toInt() ?: 0

    var totalSecs = hours.toLong()
    totalSecs = totalSecs * 60 + mins
    totalSecs = totalSecs * 60 + secs

    var totalNanos = millis.toLong()
    totalNanos = totalNanos * 1000 + micros
    totalNanos = totalNanos * 1000 + nanos

    return Duration.ofSeconds(totalSecs, totalNanos)
}

fun Duration.toHumanString(): String {
    var totalSecs = this.seconds
    var totalNanos = this.nano

    return buildString {
        val secs = totalSecs % 60
        totalSecs /= 60
        val mins = totalSecs % 60
        totalSecs /= 60
        val hours = totalSecs

        if (hours > 0) append("${hours}h")
        if (mins > 0) append("${mins}m")
        if (secs > 0) append("${secs}s")

        val nanos = totalNanos % 1000
        totalNanos /= 1000
        val micros = totalNanos % 1000
        totalNanos /= 1000
        val millis = totalNanos

        if (millis > 0) append("${millis}ms")
        if (micros > 0) append("${micros}us")
        if (nanos > 0) append("${nanos}ns")
    }
}

class DurationSerializer : StdSerializer<Duration>(Duration::class.java) {
    override fun serialize(value: Duration, gen: JsonGenerator, provider: SerializerProvider) {
        val str = value.toHumanString()
        gen.writeString(str)
    }
}

class DurationDeserializer : StdDeserializer<Duration>(Duration::class.java) {
    override fun deserialize(parser: JsonParser, ctx: DeserializationContext): Duration {
        val str = ctx.readValue(parser, String::class.java)
        return str.toDuration()
    }
}
