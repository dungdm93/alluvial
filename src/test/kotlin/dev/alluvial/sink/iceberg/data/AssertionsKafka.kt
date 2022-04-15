package dev.alluvial.sink.iceberg.data

import dev.alluvial.utils.LocalDateTimes
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
import java.time.OffsetTime
import java.time.ZoneOffset
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
                val expectedMillis = MILLIS.floorConvert((expected as LocalTime).toNanoOfDay(), NANOS)
                expectThat(expectedMillis).isEqualTo((actual as Int).toLong())
            }
            io.debezium.time.MicroTime.SCHEMA_NAME -> {
                val expectedMicros = MICROS.floorConvert((expected as LocalTime).toNanoOfDay(), NANOS)
                expectThat(expectedMicros).isEqualTo(actual as Long)
            }
            io.debezium.time.NanoTime.SCHEMA_NAME -> {
                val expectedMicros = MICROS.floorConvert((expected as LocalTime).toNanoOfDay(), NANOS)
                val actualMicros = MICROS.floorConvert(actual as Long, NANOS)
                // Iceberg stores time type with micros precision
                expectThat(expectedMicros).isEqualTo(actualMicros)
            }
            io.debezium.time.ZonedTime.SCHEMA_NAME -> {
                val expectedOt = OffsetTime.of(expected as LocalTime, ZoneOffset.UTC)
                val actualOt = OffsetTime.parse(actual as String).truncatedTo(ChronoUnit.MICROS)
                expectThat(expectedOt.isEqual(actualOt)).isTrue()
            }
            io.debezium.time.Timestamp.SCHEMA_NAME -> {
                val expectedMillis = MILLIS.floorConvert(LocalDateTimes.toEpochNano(expected as LocalDateTime), NANOS)
                expectThat(expectedMillis).isEqualTo(actual as Long)
            }
            io.debezium.time.MicroTimestamp.SCHEMA_NAME -> {
                val expectedMicros = MICROS.floorConvert(LocalDateTimes.toEpochNano(expected as LocalDateTime), NANOS)
                expectThat(expectedMicros).isEqualTo((actual as Number).toLong())
            }
            io.debezium.time.NanoTimestamp.SCHEMA_NAME -> {
                val expectedMicros = MICROS.floorConvert(LocalDateTimes.toEpochNano(expected as LocalDateTime), NANOS)
                val actualMicros = MICROS.floorConvert(actual as Long, NANOS)
                // Iceberg stores timestamp type with micros precision
                expectThat(expectedMicros).isEqualTo(actualMicros)
            }
            io.debezium.time.ZonedTimestamp.SCHEMA_NAME -> {
                val actualOdt = OffsetDateTime.parse(actual as String).truncatedTo(ChronoUnit.MICROS)
                expectThat((expected as OffsetDateTime).isEqual(actualOdt)).isTrue()
            }

            /////////////// Kafka Logical Types ///////////////
            org.apache.kafka.connect.data.Date.LOGICAL_NAME -> {
                val expectedDays = (expected as LocalDate).toEpochDay()
                val actualDays = TimeUnit.MILLISECONDS.toDays((actual as Date).time)
                expectThat(expectedDays).isEqualTo(actualDays)
            }
            org.apache.kafka.connect.data.Time.LOGICAL_NAME -> {
                val expectedMillis = MILLIS.convert((expected as LocalTime).toNanoOfDay(), NANOS)
                val actualMillis = (actual as Date).time
                expectThat(expectedMillis).isEqualTo(actualMillis)
            }
            org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME -> {
                val expectedInstant = if ((icebergType as Types.TimestampType).shouldAdjustToUTC())
                    (expected as OffsetDateTime).toInstant() else
                    (expected as LocalDateTime).toInstant(ZoneOffset.UTC)
                expectThat(Date.from(expectedInstant)).isEqualTo(actual as Date)
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
                val e = when (expected) {
                    null -> null
                    is ByteBuffer -> expected
                    is ByteArray -> ByteBuffer.wrap(expected)
                    else -> throw AssertionError("Unexpected kind ${expected.javaClass}")
                }
                val a = when (actual) {
                    null -> null
                    is ByteBuffer -> actual
                    is ByteArray -> ByteBuffer.wrap(actual)
                    else -> throw AssertionError("Unexpected kind ${actual.javaClass}")
                }
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
}
