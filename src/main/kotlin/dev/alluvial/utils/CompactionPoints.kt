package dev.alluvial.utils

import dev.alluvial.runtime.CompactionRules
import org.apache.iceberg.Snapshot
import org.apache.iceberg.sourceTimestampMillis
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class CompactionPoints(
    val tz: ZoneOffset,
    val hourCompactionPoint: Instant,
    val dayCompactionPoint: Instant,
    val monthCompactionPoint: Instant,
    val yearCompactionPoint: Instant,
) {
    companion object {
        private val RAW_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME
        private val HOUR_COMPACT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH")
        private val DAY_COMPACT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        private val MONTH_COMPACT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM")
        private val YEAR_COMPACT_FORMATTER = DateTimeFormatter.ofPattern("yyyy")

        fun from(now: ZonedDateTime, rules: CompactionRules): CompactionPoints {
            val midnight = now.with(LocalTime.MIDNIGHT)

            val hourCompactionPoint = now.minusHours(rules.retainRaw)
                .truncatedTo(ChronoUnit.HOURS)
                .toInstant()
            val dayCompactionPoint = now.minusHours(rules.retainHourCompact)
                .with(LocalTime.MIDNIGHT)
                .toInstant()

            val monthCompactionPoint = midnight.minusDays(rules.retainDayCompact)
                .withDayOfMonth(1)
                .toInstant()
            val yearCompactionPoint = midnight.minusMonths(rules.retainMonthCompact)
                .withDayOfYear(1)
                .toInstant()

            return CompactionPoints(
                rules.tz,
                hourCompactionPoint,
                dayCompactionPoint,
                monthCompactionPoint,
                yearCompactionPoint,
            )
        }
    }

    fun keyOf(snapshot: Snapshot): String {
        val ts = Instant.ofEpochMilli(snapshot.sourceTimestampMillis())
        val zdt = ZonedDateTimes.ofEpochTime(snapshot.sourceTimestampMillis(), TimePrecision.MILLIS, tz)

        return when {
            hourCompactionPoint <= ts -> RAW_FORMATTER.format(zdt)
            dayCompactionPoint <= ts -> HOUR_COMPACT_FORMATTER.format(zdt)
            monthCompactionPoint <= ts -> DAY_COMPACT_FORMATTER.format(zdt)
            yearCompactionPoint <= ts -> MONTH_COMPACT_FORMATTER.format(zdt)
            else -> YEAR_COMPACT_FORMATTER.format(zdt)
        }
    }
}
