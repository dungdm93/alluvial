package dev.alluvial.utils

import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.ZonedDateTime

const val MILLIS_IN_SECOND = 1_000L
const val MICROS_IN_SECOND = 1_000_000L
const val NANOS_IN_SECOND = 1_000_000_000L

// java.time.LocalDate.ofEpochDay
// java.time.LocalDate.toEpochDay

// java.time.LocalTime.ofNanoOfDay
// java.time.LocalTime.toNanoOfDay

object OffsetTimes {
    /**
     * @return [OffsetTime] from nano of day in given timezone
     */
    fun ofNanoOfDay(nanoOfDay: Long, tz: ZoneOffset = ZoneOffset.UTC): OffsetTime {
        val localTime = LocalTime.ofNanoOfDay(nanoOfDay)
        return OffsetTime.of(localTime, tz)
    }

    /**
     * @return the nano of day in given timezone.
     *      if timezone is none, return in current timezone
     */
    fun toNanoOfDay(offsetTime: OffsetTime, tz: ZoneOffset? = null): Long {
        val localTimeNanos = offsetTime.toLocalTime().toNanoOfDay()
        if (tz == null) return localTimeNanos

        val diffOffsetSeconds = (offsetTime.offset.totalSeconds - tz.totalSeconds).toLong()
        return localTimeNanos - diffOffsetSeconds * NANOS_IN_SECOND
    }
}

object LocalDateTimes {
    /**
     * @return [LocalDateTime] from `epochNano` in given timezone
     */
    fun ofEpochNano(epochNano: Long, tz: ZoneOffset = ZoneOffset.UTC): LocalDateTime {
        val epochSecond = Math.floorDiv(epochNano, NANOS_IN_SECOND)
        val nanoOfSecond = Math.floorMod(epochNano, NANOS_IN_SECOND).toInt()
        return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, tz)
    }

    /**
     * @return the number of nanos since epoch from `localDateTime` given timezone
     */
    fun toEpochNano(localDateTime: LocalDateTime, tz: ZoneOffset = ZoneOffset.UTC): Long {
        val epochSecond = localDateTime.toEpochSecond(tz)
        val nanoOfSecond = localDateTime.nano
        return epochSecond * NANOS_IN_SECOND + nanoOfSecond
    }
}

object OffsetDateTimes {
    /**
     * @return [OffsetDateTime] from `epochNano` and timezone
     */
    fun ofEpochNano(epochNano: Long, tz: ZoneOffset = ZoneOffset.UTC): OffsetDateTime {
        val epochSecond = Math.floorDiv(epochNano, NANOS_IN_SECOND)
        val nanoOfSecond = Math.floorMod(epochNano, NANOS_IN_SECOND).toInt()
        val localDateTime = LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, tz)
        return OffsetDateTime.of(localDateTime, tz)
    }

    /**
     * @return the number of nanos since epoch from `offsetDateTime`
     */
    fun toEpochNano(offsetDateTime: OffsetDateTime): Long {
        val epochSecond = offsetDateTime.toEpochSecond()
        val nanoOfSecond = offsetDateTime.nano
        return epochSecond * NANOS_IN_SECOND + nanoOfSecond
    }
}

object ZonedDateTimes {
    /**
     * @return [ZonedDateTime] from `epochNano` and timezone
     */
    fun ofEpochNano(epochNano: Long, tz: ZoneOffset = ZoneOffset.UTC): ZonedDateTime {
        val epochSecond = Math.floorDiv(epochNano, NANOS_IN_SECOND)
        val nanoOfSecond = Math.floorMod(epochNano, NANOS_IN_SECOND).toInt()
        val localDateTime = LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, tz)
        return ZonedDateTime.of(localDateTime, tz)
    }

    /**
     * @return the number of nanos since epoch from `zonedDateTime`
     */
    fun toEpochNano(zonedDateTime: ZonedDateTime): Long {
        val epochSecond = zonedDateTime.toEpochSecond()
        val nanoOfSecond = zonedDateTime.nano
        return epochSecond * NANOS_IN_SECOND + nanoOfSecond
    }
}

object Instants {
    /**
     * @return [Instant] from given `epochNano`
     */
    fun ofEpochNano(epochNano: Long): Instant {
        val epochSecond = Math.floorDiv(epochNano, NANOS_IN_SECOND)
        val nanoOfSecond = Math.floorMod(epochNano, NANOS_IN_SECOND)
        return Instant.ofEpochSecond(epochSecond, nanoOfSecond)
    }

    /**
     * @return the number of nanos since epoch from `instant`
     */
    fun toEpochNano(instant: Instant): Long {
        val epochSecond = instant.epochSecond
        val nanoOfSecond = instant.nano
        return epochSecond * NANOS_IN_SECOND + nanoOfSecond
    }
}
