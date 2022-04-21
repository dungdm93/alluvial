package dev.alluvial.sink.iceberg.data

import dev.alluvial.utils.LocalDateTimes
import dev.alluvial.utils.LocalTimes
import dev.alluvial.utils.OffsetDateTimes
import dev.alluvial.utils.TimePrecision.*
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isTrue
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.Date
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.data.Record as IcebergRecord
import org.apache.iceberg.types.Type as IcebergType
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

/** Assert Kafka Type equals Iceberg Type **/
object AssertionsKafka {
    fun assertEquals(
        icebergSchema: IcebergSchema,
        kafkaSchema: KafkaSchema,
        expected: IcebergRecord,
        actual: KafkaStruct
    ) {
        expectThat(kafkaSchema).isEqualTo(actual.schema())
        assertEquals(icebergSchema.asStruct(), kafkaSchema, expected, actual)
    }

    fun assertEquals(struct: Types.StructType, kafkaSchema: KafkaSchema, expected: IcebergRecord, actual: KafkaStruct) {
        struct.fields().forEach { field ->
            val icebergFieldType = field.type()
            val kafkaFieldSchema = kafkaSchema.field(field.name()).schema()

            val expectedValue = expected.getField(field.name())
            val actualValue = actual[field.name()]

            assertEquals(icebergFieldType, kafkaFieldSchema, expectedValue, actualValue)
        }
    }

    fun assertEquals(list: Types.ListType, kafkaSchema: KafkaSchema, expected: List<*>, actual: List<*>) {
        expectThat(actual.size)
            .describedAs("List size should match")
            .isEqualTo(expected.size)

        val elementType = list.elementType()
        expected.zip(actual) { e, a ->
            assertEquals(elementType, kafkaSchema.valueSchema(), e, a)
        }
    }

    fun assertEquals(map: Types.MapType, kafkaSchema: KafkaSchema, expected: Map<*, *>, actual: Map<*, *>) {
        expectThat(actual.size)
            .describedAs("Map size should match")
            .isEqualTo(expected.size)

        val actualKey = { ek: Any? ->
            actual.keys.first { ak: Any? ->
                try {
                    assertEquals(map.keyType(), kafkaSchema.keySchema(), ek, ak)
                    true
                } catch (e: AssertionError) {
                    false
                }
            }
        }

        expected.forEach { (ek, ev) ->
            val ak = actualKey(ek)
            if (ek != null) expectThat(ak).isNotNull()
            val av = actual[ak]
            assertEquals(map.valueType(), kafkaSchema.valueSchema(), ev, av)
        }
    }

    fun assertEquals(
        icebergType: IcebergType,
        kafkaSchema: KafkaSchema,
        expected: Any?, // Iceberg structure
        actual: Any?,   // Kafka structure
    ) {
        if (expected == null && actual == null) {
            return
        }
        if ((expected == null) xor (actual == null)) {
            throw AssertionError("Not the same nullity")
        }
        assertEqualsByKafkaSchema(icebergType, kafkaSchema, expected, actual) ||
            assertEqualsByIcebergType(icebergType, kafkaSchema, expected, actual) ||
            throw IllegalArgumentException("Unknown TypeID ${icebergType.typeId()}")
    }

    private fun assertEqualsByKafkaSchema(
        icebergType: IcebergType,
        kafkaSchema: KafkaSchema,
        expected: Any?, // Iceberg structure
        actual: Any?,   // Kafka structure
    ): Boolean {
        when (kafkaSchema.name()) {
            /////////////// Debezium Logical Types ///////////////
            io.debezium.time.Date.SCHEMA_NAME -> {
                val expectedDays = (expected as LocalDate).toEpochDay()
                expectThat(expectedDays).isEqualTo((actual as Int).toLong())
            }
            io.debezium.time.Time.SCHEMA_NAME -> {
                val expectedMillis = LocalTimes.toMidnightTime(expected as LocalTime, MILLIS)
                expectThat(expectedMillis).isEqualTo((actual as Int).toLong())
            }
            io.debezium.time.MicroTime.SCHEMA_NAME -> {
                val expectedMicros = LocalTimes.toMidnightTime(expected as LocalTime, MICROS)
                expectThat(expectedMicros).isEqualTo(actual as Long)
            }
            io.debezium.time.NanoTime.SCHEMA_NAME -> {
                // Iceberg stores time type with micros precision
                val expectedMicros = LocalTimes.toMidnightTime(expected as LocalTime, MICROS)
                val actualMicros = MICROS.floorConvert(actual as Long, NANOS)
                expectThat(expectedMicros).isEqualTo(actualMicros)
            }
            io.debezium.time.ZonedTime.SCHEMA_NAME -> {
                expectThat(expected as String).isEqualTo(actual as String) // TODO(ZonedTime): can save it as number?
                // val expectedOt = OffsetTime.of(expected as LocalTime, ZoneOffset.UTC)
                // val actualOt = OffsetTime.parse(actual as String).truncatedTo(ChronoUnit.MICROS)
                // expectThat(expectedOt.isEqual(actualOt)).isTrue()
            }
            io.debezium.time.Timestamp.SCHEMA_NAME -> {
                val expectedMillis = LocalDateTimes.toLocalEpochTime(expected as LocalDateTime, MILLIS)
                expectThat(expectedMillis).isEqualTo(actual as Long)
            }
            io.debezium.time.MicroTimestamp.SCHEMA_NAME -> {
                val expectedMicros = LocalDateTimes.toLocalEpochTime(expected as LocalDateTime, MICROS)
                expectThat(expectedMicros).isEqualTo(actual as Long)
            }
            io.debezium.time.NanoTimestamp.SCHEMA_NAME -> {
                // Iceberg stores timestamp type with micros precision
                val expectedMicros = LocalDateTimes.toLocalEpochTime(expected as LocalDateTime, MICROS)
                val actualMicros = MICROS.floorConvert(actual as Long, NANOS)
                expectThat(expectedMicros).isEqualTo(actualMicros)
            }
            io.debezium.time.ZonedTimestamp.SCHEMA_NAME -> {
                val actualOdt = OffsetDateTime.parse(actual as String).truncatedTo(ChronoUnit.MICROS)
                expectThat((expected as OffsetDateTime).isEqual(actualOdt)).isTrue()
            }
            io.debezium.data.Enum.LOGICAL_NAME -> expectThat(expected as String).isEqualTo(actual as String)
            io.debezium.data.EnumSet.LOGICAL_NAME -> {
                val actualAsSet = if ((actual as String).isEmpty())
                    emptySet() else
                    actual.split(",").toSet()
                val expectedAsList = expected as List<*>
                val expectedAsSet = expectedAsList.toSet()
                expectThat(expectedAsList.size).isEqualTo(expectedAsSet.size)
                expectThat(expectedAsSet).isEqualTo(actualAsSet)
            }

            // Spatial types
            io.debezium.data.geometry.Geometry.LOGICAL_NAME -> {
                val wkbFieldName = io.debezium.data.geometry.Geometry.WKB_FIELD
                val sridFieldName = io.debezium.data.geometry.Geometry.SRID_FIELD

                val actualStruct = actual as KafkaStruct
                val expectedRecord = expected as IcebergRecord
                expectThat(actualStruct.get(sridFieldName)).isEqualTo(expectedRecord.getField(sridFieldName))

                val actualWkb = toByteBuffer(actualStruct.get(wkbFieldName))
                val expectedWkb = toByteBuffer(expectedRecord.getField(wkbFieldName))
                expectThat(actualWkb).isEqualTo(expectedWkb)
            }

            /////////////// Kafka Logical Types ///////////////
            org.apache.kafka.connect.data.Date.LOGICAL_NAME -> {
                val expectedDays = (expected as LocalDate).toEpochDay()
                val actualDays = TimeUnit.MILLISECONDS.toDays((actual as Date).time)
                expectThat(expectedDays).isEqualTo(actualDays)
            }
            org.apache.kafka.connect.data.Time.LOGICAL_NAME -> {
                val expectedMillis = LocalTimes.toMidnightTime(expected as LocalTime, MILLIS)
                val actualMillis = (actual as Date).time
                expectThat(expectedMillis).isEqualTo(actualMillis)
            }
            org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME -> {
                val expectedMillis = if ((icebergType as Types.TimestampType).shouldAdjustToUTC())
                    OffsetDateTimes.toEpochTime(expected as OffsetDateTime, MILLIS) else
                    LocalDateTimes.toLocalEpochTime(expected as LocalDateTime, MILLIS)
                val actualMillis = (actual as Date).time
                expectThat(expectedMillis).isEqualTo(actualMillis)
            }
            else -> return false
        }
        return true
    }

    private fun assertEqualsByIcebergType(
        icebergType: IcebergType,
        kafkaSchema: KafkaSchema,
        expected: Any?,
        actual: Any?
    ): Boolean {
        when (icebergType.typeId()) {
            TypeID.INTEGER -> // actual can be Bytes, Short or Integer
                expectThat(expected).isEqualTo((actual as Number).toInt())
            TypeID.BOOLEAN,
            TypeID.LONG,
            TypeID.FLOAT,
            TypeID.DOUBLE -> expectThat(expected).isEqualTo(actual)
            TypeID.DATE -> TODO()
            TypeID.TIME -> TODO()
            TypeID.TIMESTAMP -> TODO()
            TypeID.STRING -> expectThat((expected as CharSequence).toString()).isEqualTo((actual as CharSequence).toString())
            TypeID.UUID -> expectThat(expected as UUID).isEqualTo(actual as UUID)
            TypeID.FIXED -> expectThat(expected as ByteArray).isEqualTo(actual as ByteArray)
            TypeID.BINARY -> {
                val e = toByteBuffer(expected)
                val a = toByteBuffer(actual)
                expectThat(e).isEqualTo(a)
            }
            TypeID.DECIMAL -> expectThat(expected as BigDecimal).isEqualTo(actual as BigDecimal)
            TypeID.STRUCT -> assertEquals(
                icebergType.asStructType(),
                kafkaSchema,
                expected as IcebergRecord,
                actual as KafkaStruct
            )
            TypeID.LIST -> assertEquals(
                icebergType.asListType(),
                kafkaSchema,
                expected as List<*>,
                actual as List<*>
            )
            TypeID.MAP -> assertEquals(
                icebergType.asMapType(),
                kafkaSchema,
                expected as Map<*, *>,
                actual as Map<*, *>
            )
            else -> return false
        }
        return true
    }

    private fun toByteBuffer(value: Any?): ByteBuffer? {
        return when (value) {
            null -> null
            is ByteBuffer -> value
            is ByteArray -> ByteBuffer.wrap(value)
            else -> throw AssertionError("Unexpected kind ${value.javaClass}")
        }
    }
}
