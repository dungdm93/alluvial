package dev.alluvial.utils

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isTrue
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.stream.Stream

internal class TestTime {
    companion object {
        val timezones = arrayOf(
            ZoneOffset.UTC,
            ZoneOffset.of("+07:00"), ZoneOffset.of("-07:00"),
            ZoneOffset.of("+01:15"), ZoneOffset.of("-09:45"),
            ZoneOffset.of("+05:30"), ZoneOffset.of("-12:25"),
        )

        @JvmStatic
        fun offsetTimes(): Stream<Arguments> = Stream.of(
            Arguments.of("00:00:00Z"), // EPOCH
            Arguments.of("00:00:00.123456789Z"), // < 1s from epoch
            Arguments.of("23:59:59.987654321Z"), // < 1s to epoch

            Arguments.of("01:02:03.546372819+07:00"), // < epoch in tz +7
            Arguments.of("06:59:59.987654321+07:00"), // < 1s to epoch in tz +7
            Arguments.of("07:00:00+07:00"), // EPOCH in tz +7
            Arguments.of("07:00:00.123456789+07:00"), // < 1s from epoch in tz +7
            Arguments.of("09:08:07.912837465+07:00"), // > epoch in tz +7

            Arguments.of("13:12:11.546372819-07:00"), // < epoch in tz -7
            Arguments.of("16:59:59.987654321-07:00"), // < 1s to epoch in tz -7
            Arguments.of("17:00:00-07:00"), // EPOCH in tz -7
            Arguments.of("17:00:00.123456789-07:00"), // > 1s from epoch in tz -7
            Arguments.of("20:21:22.912837465-07:00"), // > epoch in tz -7
        )

        @JvmStatic
        fun localDateTimes(): Stream<Arguments> = Stream.of(
            Arguments.of("1970-01-01T00:00:00"), // EPOCH
            Arguments.of("1970-01-01T00:00:00.123456789"), // < 1s from epoch
            Arguments.of("1969-12-31T23:59:59.987654321"), // < 1s to epoch

            Arguments.of("1970-01-01T01:02:03.546372819"), // < epoch in tz +7
            Arguments.of("1970-01-01T06:59:59.987654321"), // < 1s to epoch in tz +7
            Arguments.of("1970-01-01T07:00:00"), // EPOCH in tz +7
            Arguments.of("1970-01-01T07:00:00.123456789"), // < 1s from epoch in tz +7
            Arguments.of("1970-01-01T09:08:07.912837465"), // > epoch in tz +7

            Arguments.of("1969-12-31T13:12:11.546372819"), // < epoch in tz -7
            Arguments.of("1969-12-31T16:59:59.987654321"), // < 1s to epoch in tz -7
            Arguments.of("1969-12-31T17:00:00"), // EPOCH in tz -7
            Arguments.of("1969-12-31T17:00:00.123456789"), // > 1s from epoch in tz -7
            Arguments.of("1969-12-31T20:21:22.912837465"), // > epoch in tz -7
        )

        @JvmStatic
        fun offsetDateTimes(): Stream<Arguments> = Stream.of(
            Arguments.of("1970-01-01T00:00:00Z"), // EPOCH
            Arguments.of("1970-01-01T00:00:00.123456789Z"), // < 1s from epoch
            Arguments.of("1969-12-31T23:59:59.987654321Z"), // < 1s to epoch

            Arguments.of("1970-01-01T01:02:03.546372819+07:00"), // < epoch in tz +7
            Arguments.of("1970-01-01T06:59:59.987654321+07:00"), // < 1s to epoch in tz +7
            Arguments.of("1970-01-01T07:00:00+07:00"), // EPOCH in tz +7
            Arguments.of("1970-01-01T07:00:00.123456789+07:00"), // < 1s from epoch in tz +7
            Arguments.of("1970-01-01T09:08:07.912837465+07:00"), // > epoch in tz +7

            Arguments.of("1969-12-31T13:12:11.546372819-07:00"), // < epoch in tz -7
            Arguments.of("1969-12-31T16:59:59.987654321-07:00"), // < 1s to epoch in tz -7
            Arguments.of("1969-12-31T17:00:00-07:00"), // EPOCH in tz -7
            Arguments.of("1969-12-31T17:00:00.123456789-07:00"), // > 1s from epoch in tz -7
            Arguments.of("1969-12-31T20:21:22.912837465-07:00"), // > epoch in tz -7
        )

        // instant string representation must be in UTC
        @JvmStatic
        fun instants(): Stream<Arguments> = Stream.of(
            Arguments.of("1970-01-01T00:00:00Z"), // EPOCH
            Arguments.of("1970-01-01T00:00:00.123456789Z"), // < 1s from epoch
            Arguments.of("1969-12-31T23:59:59.987654321Z"), // < 1s to epoch
        )

        @JvmStatic
        fun parseDurations(): Stream<Arguments> = Stream.of(
            Arguments.of("1h", Duration.parse("PT1H")),
            Arguments.of("1 hour 200 minutes", Duration.parse("PT4H20M")),
            Arguments.of("3600 secs", Duration.parse("PT1H")),
            Arguments.of("2h3m4s", Duration.parse("PT2H3M4S")),

            Arguments.of("2h3m4s5ms", Duration.parse("PT2H3M4.005S")),
            Arguments.of("2h3m4s6us", Duration.parse("PT2H3M4.000006S")),
            Arguments.of("2h3m4s6µs", Duration.parse("PT2H3M4.000006S")),
            Arguments.of("2h3m4s7ns", Duration.parse("PT2H3M4.000000007S")),
            Arguments.of("2h3m4s5ms6us7ns", Duration.parse("PT2H3M4.005006007S")),
        )

        @JvmStatic
        fun formatDurations(): Stream<Arguments> = Stream.of(
            Arguments.of(Duration.parse("PT1H"), "1h"),
            Arguments.of(Duration.parse("PT4H20M"), "4h20m"),
            Arguments.of(Duration.parse("PT2H3M4S"), "2h3m4s"),
            Arguments.of(Duration.parse("PT2H3M4.005S"), "2h3m4s5ms"),
            Arguments.of(Duration.parse("PT2H3M4.000006S"), "2h3m4s6us"),
            Arguments.of(Duration.parse("PT2H3M4.000000007S"), "2h3m4s7ns"),
            Arguments.of(Duration.parse("PT2H3M4.005006007S"), "2h3m4s5ms6us7ns"),
        )
    }

    @ParameterizedTest
    @MethodSource("offsetTimes")
    fun testOffsetTimes(timeStr: String) {
        val oTime = OffsetTime.parse(timeStr)
        val oTimeNanos = OffsetTimes.toUtcMidnightTime(oTime)
        val pTime = OffsetTimes.ofUtcMidnightTime(oTimeNanos, tz = oTime.offset)
        val pTimeNanos = OffsetTimes.toUtcMidnightTime(pTime)

        expectThat(oTime).isEqualTo(pTime)
        expectThat(oTimeNanos).isEqualTo(pTimeNanos)
    }

    @ParameterizedTest
    @MethodSource("localDateTimes")
    fun testLocalDateTimes(str: String) {
        val ldt = LocalDateTime.parse(str)
        val lTimeNanos = LocalDateTimes.toLocalEpochTime(ldt)
        val mdt = LocalDateTimes.ofLocalEpochTime(lTimeNanos)
        val mTimeNanos = LocalDateTimes.toLocalEpochTime(mdt)

        expectThat(ldt.isEqual(mdt)).isTrue()
        expectThat(lTimeNanos).isEqualTo(mTimeNanos)
    }

    @ParameterizedTest
    @MethodSource("offsetDateTimes")
    fun testOffsetDateTimes(str: String) {
        val zdt = OffsetDateTime.parse(str)
        timezones.forEach { tz ->
            val zTimeNanos = OffsetDateTimes.toEpochTime(zdt)
            val ydt = OffsetDateTimes.ofEpochTime(zTimeNanos, tz = tz)
            val yTimeNanos = OffsetDateTimes.toEpochTime(ydt)

            expectThat(zdt.isEqual(ydt)).isTrue()
            expectThat(zTimeNanos).isEqualTo(yTimeNanos)
        }
    }

    @ParameterizedTest
    @MethodSource("offsetDateTimes")
    fun testZonedDateTimes(str: String) {
        val zdt = ZonedDateTime.parse(str)
        timezones.forEach { tz ->
            val zTimeNanos = ZonedDateTimes.toEpochTime(zdt)
            val ydt = ZonedDateTimes.ofEpochTime(zTimeNanos, tz = tz)
            val yTimeNanos = ZonedDateTimes.toEpochTime(ydt)

            expectThat(zdt.isEqual(ydt)).isTrue()
            expectThat(zTimeNanos).isEqualTo(yTimeNanos)
        }
    }

    @ParameterizedTest
    @MethodSource("instants")
    fun testInstants(str: String) {
        val zts = Instant.parse(str)
        val zTimeNanos = Instants.toEpochTime(zts)
        val yts = Instants.ofEpochTime(zTimeNanos)
        val yTimeNanos = Instants.toEpochTime(yts)

        expectThat(zts).isEqualTo(yts)
        expectThat(zTimeNanos).isEqualTo(yTimeNanos)
    }

    @ParameterizedTest
    @MethodSource("parseDurations")
    fun testParseDuration(str: String, expected: Duration) {
        val actual = str.toDuration()
        expectThat(actual).isEqualTo(expected)
    }

    @ParameterizedTest
    @MethodSource("formatDurations")
    fun testFormatDuration(duration: Duration, expected: String) {
        val actual = duration.toHumanString()
        expectThat(actual).isEqualTo(expected)
    }

    data class Envelop(
        @JsonSerialize(using = DurationSerializer::class)
        @JsonDeserialize(using = DurationDeserializer::class)
        var duration: Duration
    )

    private val mapper = JsonMapper().also {
        it.registerModule(KotlinModule())
    }

    @ParameterizedTest
    @MethodSource("parseDurations")
    fun testParseDurationJson(str: String, expected: Duration) {
        val json = """{ "duration": "$str" }"""
        val actual = mapper.readValue(json, Envelop::class.java)

        expectThat(actual.duration).isEqualTo(expected)
    }

    @ParameterizedTest
    @MethodSource("formatDurations")
    fun testFormatDurationJson(duration: Duration, expected: String) {
        val envelop = Envelop(duration)
        val actual = mapper.writeValueAsString(envelop)

        val json = """{"duration":"$expected"}"""
        expectThat(actual).isEqualTo(json)
    }
}
