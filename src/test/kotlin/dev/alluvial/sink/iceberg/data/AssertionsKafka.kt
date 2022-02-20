package dev.alluvial.sink.iceberg.data

import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Date
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.data.Record as IcebergRecord
import org.apache.iceberg.types.Type as IcebergType
import org.apache.kafka.connect.data.Struct as KafkaStruct

/** Assert Kafka Type equals Iceberg Type **/
object AssertionsKafka {
    fun assertEquals(
        icebergSchema: IcebergSchema,
        expected: IcebergRecord,
        actual: KafkaStruct
    ) {
        val kafkaSchema = KafkaSchemaUtil.toKafkaSchema(icebergSchema)
        expectThat(kafkaSchema).isEqualTo(actual.schema())
        assertEquals(icebergSchema.asStruct(), expected, actual)
    }

    fun assertEquals(struct: Types.StructType, expected: IcebergRecord, actual: KafkaStruct) {
        struct.fields().forEach { field ->
            val fieldType = field.type()

            val expectedValue = expected.getField(field.name())
            val actualValue = actual[field.name()]

            assertEquals(fieldType, expectedValue, actualValue)
        }
    }

    fun assertEquals(list: Types.ListType, expected: List<*>, actual: List<*>) {
        expectThat(actual.size)
            .describedAs("List size should match")
            .isEqualTo(expected.size)

        val elementType = list.elementType()
        expected.zip(actual) { e, a ->
            assertEquals(elementType, e, a)
        }
    }

    fun assertEquals(map: Types.MapType, expected: Map<*, *>, actual: Map<*, *>) {
        expectThat(actual.size)
            .describedAs("Map size should match")
            .isEqualTo(expected.size)

        val actualKey = { ek: Any? ->
            actual.keys.first { ak: Any? ->
                try {
                    assertEquals(map.keyType(), ek, ak)
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
            assertEquals(map.valueType(), ev, av)
        }
    }


    fun assertEquals(type: IcebergType, expected: Any?, actual: Any?) {
        if (expected == null && actual == null) {
            return
        }
        when (type.typeId()) {
            TypeID.BOOLEAN,
            TypeID.INTEGER,
            TypeID.LONG,
            TypeID.FLOAT,
            TypeID.DOUBLE -> expectThat(expected).isEqualTo(actual)
            TypeID.DATE -> {
                val expectedDays = (expected as LocalDate).toEpochDay()
                val actualDays = TimeUnit.MILLISECONDS.toDays((actual as Date).time)
                expectThat(expectedDays).isEqualTo(actualDays)
            }
            TypeID.TIME -> {
                val expectedMillis = TimeUnit.NANOSECONDS.toMillis((expected as LocalTime).toNanoOfDay())
                val actualMillis = (actual as Date).time
                expectThat(expectedMillis).isEqualTo(actualMillis)
            }
            TypeID.TIMESTAMP -> {
                val expectedInstant = if ((type as Types.TimestampType).shouldAdjustToUTC())
                    (expected as OffsetDateTime).toInstant() else
                    (expected as LocalDateTime).toInstant(ZoneOffset.UTC)
                expectThat(Date.from(expectedInstant)).isEqualTo(actual as Date)
            }
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
            TypeID.STRUCT -> {
                expectThat(expected).isA<IcebergRecord>()
                expectThat(actual).isA<KafkaStruct>()
                assertEquals(type.asStructType(), expected as IcebergRecord, actual as KafkaStruct)
            }
            TypeID.LIST -> {
                expectThat(expected).isA<List<*>>()
                expectThat(actual).isA<List<*>>()
                assertEquals(type.asListType(), expected as List<*>, actual as List<*>)
            }
            TypeID.MAP -> {
                expectThat(expected).isA<Map<*, *>>()
                expectThat(actual).isA<Map<*, *>>()
                assertEquals(type.asMapType(), expected as Map<*, *>, actual as Map<*, *>)
            }
            else -> throw IllegalArgumentException("Unknown TypeID ${type.typeId()}")
        }
    }
}
