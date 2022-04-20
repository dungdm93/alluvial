package dev.alluvial.utils

import dev.alluvial.utils.TimePrecision.*
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime

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
