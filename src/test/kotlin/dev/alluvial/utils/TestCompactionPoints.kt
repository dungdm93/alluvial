package dev.alluvial.utils

import dev.alluvial.runtime.CompactionRules
import org.apache.iceberg.SOURCE_TIMESTAMP_PROP
import org.apache.iceberg.Snapshot
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import java.time.ZonedDateTime

internal class TestCompactionPoints {
    @Test
    fun defaultCompactionRules() {
        val rules = CompactionRules()
        val now = ZonedDateTime.parse("2022-07-15T12:14:16.432Z")
        val points = CompactionPoints.from(now, rules)

        Assertions.assertEquals(
            ZonedDateTime.parse("2022-07-15T09:00:00Z").toInstant(),
            points.hourCompactionPoint
        )
        Assertions.assertEquals(
            ZonedDateTime.parse("2022-07-14T00:00:00Z").toInstant(),
            points.dayCompactionPoint
        )
        Assertions.assertEquals(
            ZonedDateTime.parse("2022-06-01T00:00:00Z").toInstant(),
            points.monthCompactionPoint
        )
        Assertions.assertEquals(
            ZonedDateTime.parse("2021-01-01T00:00:00Z").toInstant(),
            points.yearCompactionPoint
        )
    }

    @Test
    fun compactionPointsInSameTimeUnit() {
        val rules = CompactionRules(
            retainRaw = 3,
            retainHourCompact = 12,
            retainDayCompact = 15,
            retainMonthCompact = 6
        )
        val now = ZonedDateTime.parse("2021-12-25T19:20:45.078Z")
        val points = CompactionPoints.from(now, rules)
        Assertions.assertEquals(
            ZonedDateTime.parse("2021-12-25T16:00:00Z").toInstant(),
            points.hourCompactionPoint
        )
        Assertions.assertEquals(
            ZonedDateTime.parse("2021-12-25T00:00:00Z").toInstant(),
            points.dayCompactionPoint
        )
        Assertions.assertEquals(
            ZonedDateTime.parse("2021-12-01T00:00:00Z").toInstant(),
            points.monthCompactionPoint
        )
        Assertions.assertEquals(
            ZonedDateTime.parse("2021-01-01T00:00:00Z").toInstant(),
            points.yearCompactionPoint
        )
    }

    @Test
    fun getKeyOfSnapshot() {
        val rules = CompactionRules()
        val now = ZonedDateTime.parse("2022-07-15T12:14:16.432Z")
        val points = CompactionPoints.from(now, rules)

        // Raw
        testKeyFor(points, "2022-07-15T10:00:00Z", "2022-07-15T10:00:00")
        testKeyFor(points, "2022-07-15T09:00:00Z", "2022-07-15T09:00:00")

        // Hour
        testKeyFor(points, "2022-07-15T08:59:59Z", "2022-07-15T08")
        testKeyFor(points, "2022-07-14T00:00:00Z", "2022-07-14T00")

        // Day
        testKeyFor(points, "2022-07-13T23:59:59Z", "2022-07-13")
        testKeyFor(points, "2022-06-01T00:00:00Z", "2022-06-01")

        // Month
        testKeyFor(points, "2022-05-31T23:59:59Z", "2022-05")
        testKeyFor(points, "2021-01-01T00:00:00Z", "2021-01")

        // Year
        testKeyFor(points, "2020-12-31T23:59:59Z", "2020")
        testKeyFor(points, "2020-05-06T07:08:09Z", "2020")
    }

    private fun testKeyFor(points: CompactionPoints, input: String, key: String) {
        val ts = ZonedDateTime.parse(input).toInstant().toEpochMilli()
        val snapshot = snapshotOfTimestamp(ts)
        Assertions.assertEquals(key, points.keyOf(snapshot))
    }

    private fun snapshotOfTimestamp(timestampMillis: Long): Snapshot {
        return mock {
            on { timestampMillis() } doReturn timestampMillis
            on { summary() } doReturn mapOf(SOURCE_TIMESTAMP_PROP to timestampMillis.toString())
        }
    }
}
