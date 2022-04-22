package dev.alluvial.sink.iceberg.data

import dev.alluvial.source.kafka.ENUM_VALUES
import dev.alluvial.utils.OffsetDateTimes
import dev.alluvial.utils.OffsetTimes
import dev.alluvial.utils.TimePrecision
import dev.alluvial.utils.TimePrecision.*
import org.apache.iceberg.relocated.com.google.common.collect.Sets
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.types.Types
import org.apache.iceberg.util.RandomUtil
import java.math.BigDecimal
import java.math.BigInteger
import java.time.ZoneOffset
import java.util.Date
import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Type.TypeID as IcebergType
import org.apache.kafka.connect.data.Field as KafkaField
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Schema.Type as KafkaType
import org.apache.kafka.connect.data.Struct as KafkaStruct


internal object KafkaRandomDataGenerator {
    private const val FIFTY_YEARS_IN_NANOS = 50L * (365 * 3 + 366) * 24 * 60 * 60 * 1_000_000_000 / 4
    private const val ABOUT_380_YEARS_IN_DAYS = 380 * 365
    private const val A_HUNDRED_YEAR = 100
    private const val ONE_DAY_IN_NANOS = 24 * 60 * 60 * 1_000_000_000L
    private const val CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?"
    private const val DIGITS = "0123456789"

    private fun negate(num: Int) = num % 2 == 1
    private fun Random.nextByte() = nextInt(Byte.MAX_VALUE + 1).toByte()
    private fun Random.nextShort() = nextInt(Short.MAX_VALUE + 1).toShort()
    private fun randomString(random: Random): String {
        val length = random.nextInt(50)
        val buffer = ByteArray(length)
        repeat(length) { i ->
            buffer[i] = CHARS[random.nextInt(CHARS.length)].code.toByte()
        }
        return String(buffer)
    }

    private fun randomUnscaled(precision: Int, random: Random): BigInteger? {
        val length = random.nextInt(precision)
        if (length == 0) return BigInteger.ZERO
        val sb = StringBuilder()
        repeat(length) {
            sb.append(DIGITS[random.nextInt(DIGITS.length)])
        }
        return BigInteger(sb.toString())
    }

    private fun dateFrom(days: Int): Date {
        val time = TimeUnit.DAYS.toMillis(days.toLong())
        return Date(time)
    }

    private fun datetimeFrom(timeMicros: Long): Date {
        val timeMillis = MILLIS.floorConvert(timeMicros, MICROS)
        return Date(timeMillis)
    }

    /**
     * Generate [KafkaStruct] based on given [IcebergSchema]
     */
    internal class BasedOnIcebergSchema(
        seed: Long,
        private val referenceSchema: KafkaSchema
    ) : TypeUtil.CustomOrderSchemaVisitor<Any>() {
        private var sourceSchema: KafkaSchema? = null
        private val random = Random(seed)

        private inline fun <T> Supplier<T>.getUnless(condition: (T) -> Boolean): T {
            var v = this.get()
            while (condition(v)) {
                v = this.get()
            }
            return v
        }

        private inline fun <T> travel(schema: KafkaSchema, block: () -> T): T {
            val previousSchema = sourceSchema
            try {
                sourceSchema = schema
                return block()
            } finally {
                sourceSchema = previousSchema
            }
        }

        override fun schema(schema: IcebergSchema, structResult: Supplier<Any?>): KafkaStruct =
            travel(referenceSchema) {
                return structResult.get() as KafkaStruct
            }

        override fun struct(struct: Types.StructType, fieldResults: Iterable<Any?>): KafkaStruct {
            val s = KafkaStruct(sourceSchema)
            fieldResults.forEachIndexed { idx, fieldResult ->
                val field = sourceSchema!!.fields()[idx]
                s.put(field, fieldResult)
            }
            return s
        }

        override fun field(field: Types.NestedField, fieldResult: Supplier<Any?>): Any? =
            travel(sourceSchema!!.field(field.name()).schema()) {
                // return null 5% of the time when the value is optional
                return if (field.isOptional && random.nextInt(20) == 1)
                    null else fieldResult.get()
            }

        override fun list(list: Types.ListType, elementResult: Supplier<Any?>): List<Any?> =
            travel(sourceSchema!!.valueSchema()) {
                val numElements = random.nextInt(20)

                return buildList(numElements) {
                    repeat(numElements) {
                        // return null 5% of the time when the value is optional
                        val element = if (list.isElementOptional && random.nextInt(20) == 1)
                            null else elementResult.get()
                        add(element)
                    }
                }
            }

        override fun map(map: Types.MapType, keyResult: Supplier<Any?>, valueResult: Supplier<Any?>): Map<Any?, Any?> {
            val numEntries = random.nextInt(20)

            val keySet = Sets.newHashSet<Any>()
            return buildMap(numEntries) {
                repeat(numEntries) {
                    val key = travel(sourceSchema!!.keySchema()) {
                        keyResult.getUnless(keySet::contains)
                    }
                    keySet.add(key)

                    // return null 5% of the time when the value is optional
                    val value = travel(sourceSchema!!.valueSchema()) {
                        if (map.isValueOptional && random.nextInt(20) == 1)
                            null else valueResult.get()
                    }
                    put(key, value)
                }
            }
        }

        override fun primitive(primitive: Type.PrimitiveType): Any {
            val obj = RandomUtil.generatePrimitive(primitive, random)
            return when (primitive.typeId()) {
                IcebergType.DATE -> dateFrom(obj as Int)
                IcebergType.TIME -> datetimeFrom(obj as Long)
                IcebergType.TIMESTAMP -> datetimeFrom(obj as Long)
                else -> obj
            }
        }
    }

    /**
     * Generate [KafkaStruct] based on given [KafkaSchema]
     */
    internal class BasedOnKafkaSchema(seed: Long) {
        private val random = Random(seed)

        private fun shouldNull(schema: KafkaSchema): Boolean {
            return schema.isOptional && random.nextInt(20) == 1
        }

        private inline fun generateUnless(schema: KafkaSchema, condition: (Any) -> Boolean): Any {
            var v = generate(schema)
            while (condition(v)) {
                v = generate(schema)
            }
            return v
        }

        private fun struct(struct: KafkaSchema): KafkaStruct {
            val s = KafkaStruct(struct)

            struct.fields().forEach { field ->
                val value = field(field)
                s.put(field, value)
            }
            return s
        }

        private fun field(field: KafkaField): Any? {
            // return null 5% of the time when the value is optional
            return if (shouldNull(field.schema()))
                null else generate(field.schema())
        }

        private fun map(map: KafkaSchema): Map<*, *> {
            val numEntries = random.nextInt(20)
            val keySchema = map.keySchema()
            val valueSchema = map.valueSchema()

            val keySet = Sets.newHashSet<Any>()
            return buildMap {
                repeat(numEntries) {
                    val key = generateUnless(keySchema, keySet::contains)

                    // return null 5% of the time when the value is optional
                    val value = if (shouldNull(valueSchema))
                        null else generate(valueSchema)

                    put(key, value)
                }
            }
        }

        private fun list(list: KafkaSchema): List<*> {
            val numEntries = random.nextInt(20)
            val elementSchema = list.valueSchema()

            return buildList {
                repeat(numEntries) {
                    // return null 5% of the time when the value is optional
                    val element = if (shouldNull(elementSchema))
                        null else generate(elementSchema)
                    add(element)
                }
            }
        }

        private fun randomDate(): Date {
            val days = random.nextInt() % ABOUT_380_YEARS_IN_DAYS
            return dateFrom(days)
        }

        private fun randomTime(): Date {
            val time = randomTimeAsLong(MICROS)
            return datetimeFrom(time)
        }

        private fun randomTimestamp(): Date {
            val time = randomTimestampAsLong(MICROS)
            return datetimeFrom(time)
        }

        private fun randomTimeAsLong(precision: TimePrecision): Long {
            val timeNanos = (random.nextLong() and Long.MAX_VALUE) % ONE_DAY_IN_NANOS
            return precision.convert(timeNanos, NANOS)
        }

        private fun randomTimestampAsLong(precision: TimePrecision): Long {
            val timeNanos = random.nextLong() % FIFTY_YEARS_IN_NANOS
            return precision.convert(timeNanos, NANOS)
        }

        private fun randomZone(): ZoneOffset {
            return when (random.nextInt(10)) {
                1 -> ZoneOffset.UTC
                2 -> ZoneOffset.MIN
                3 -> ZoneOffset.MAX
                else -> {
                    var second = random.nextInt(ZoneOffset.MAX.totalSeconds - ZoneOffset.MIN.totalSeconds)
                    second += ZoneOffset.MIN.totalSeconds
                    ZoneOffset.ofTotalSeconds(second)
                }
            }
        }

        private fun randomDecimal(schema: KafkaSchema, random: Random): BigDecimal {
            val precision = schema.parameters()
                .getOrDefault("connect.decimal.precision", "38").toInt()
            val scale = schema.parameters()
                .getOrDefault(org.apache.kafka.connect.data.Decimal.SCALE_FIELD, "10").toInt()

            val unscaled = randomUnscaled(precision, random)
            return BigDecimal(unscaled, scale)
        }

        private fun randomEnumSet(random: Random): List<String> {
            val allowed = ENUM_VALUES.toTypedArray().apply { this.shuffle() }
            val numElements = random.nextInt(allowed.size)

            return allowed.take(numElements)
        }

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        fun generate(schema: KafkaSchema): Any {
            val choice = random.nextInt(20)

            return when (schema.name()) {
                /////////////// Debezium Logical Types ///////////////
                io.debezium.time.Date.SCHEMA_NAME -> random.nextInt() % ABOUT_380_YEARS_IN_DAYS
                io.debezium.time.Time.SCHEMA_NAME -> randomTimeAsLong(MILLIS).toInt()
                io.debezium.time.MicroTime.SCHEMA_NAME -> randomTimeAsLong(MICROS)
                io.debezium.time.NanoTime.SCHEMA_NAME -> randomTimeAsLong(NANOS)
                io.debezium.time.ZonedTime.SCHEMA_NAME -> {
                    val timeNanos = randomTimeAsLong(NANOS)
                    val offsetTime = OffsetTimes.ofUtcMidnightTime(timeNanos)
                    return io.debezium.time.ZonedTime.toIsoString(offsetTime, null)
                }
                io.debezium.time.Timestamp.SCHEMA_NAME -> randomTimestampAsLong(MILLIS)
                io.debezium.time.MicroTimestamp.SCHEMA_NAME -> randomTimestampAsLong(MICROS)
                io.debezium.time.NanoTimestamp.SCHEMA_NAME -> randomTimestampAsLong(NANOS)
                io.debezium.time.ZonedTimestamp.SCHEMA_NAME -> {
                    val zone = randomZone()
                    val timeNanos = randomTimestampAsLong(NANOS)
                    val offsetDateTime = OffsetDateTimes.ofEpochTime(timeNanos, tz = zone)
                    return io.debezium.time.ZonedTimestamp.toIsoString(offsetDateTime, null)
                }
                io.debezium.time.Year.SCHEMA_NAME -> 1970 + random.nextInt(A_HUNDRED_YEAR)
                io.debezium.data.EnumSet.LOGICAL_NAME -> randomEnumSet(random).joinToString(",")

                // Spatial types
                io.debezium.data.geometry.Geometry.LOGICAL_NAME -> struct(schema)

                /////////////// Kafka Logical Types ///////////////
                org.apache.kafka.connect.data.Date.LOGICAL_NAME -> randomDate()
                org.apache.kafka.connect.data.Time.LOGICAL_NAME -> randomTime()
                org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME -> randomTimestamp()
                org.apache.kafka.connect.data.Decimal.LOGICAL_NAME -> randomDecimal(schema, random)
                    .let { if (negate(choice)) -it else it }

                else -> when (schema.type()) {
                    KafkaType.INT8 -> when (choice) {
                        1 -> Byte.MIN_VALUE
                        2 -> Byte.MAX_VALUE
                        3 -> 0
                        else -> random.nextByte().let { if (negate(choice)) (-it).toByte() else it }
                    }
                    KafkaType.INT16 -> when (choice) {
                        1 -> Short.MIN_VALUE
                        2 -> Short.MAX_VALUE
                        3 -> 0
                        else -> random.nextShort().let { if (negate(choice)) (-it).toShort() else it }
                    }
                    KafkaType.INT32 -> when (choice) {
                        1 -> Int.MIN_VALUE
                        2 -> Int.MAX_VALUE
                        3 -> 0
                        else -> random.nextInt().let { if (negate(choice)) -it else it }
                    }
                    KafkaType.INT64 -> when (choice) {
                        1 -> Long.MIN_VALUE
                        2 -> Long.MAX_VALUE
                        3 -> 0
                        else -> random.nextLong().let { if (negate(choice)) -it else it }
                    }
                    KafkaType.FLOAT32 -> when (choice) {
                        1 -> Float.MIN_VALUE
                        2 -> -Float.MIN_VALUE
                        3 -> Float.MAX_VALUE
                        4 -> -Float.MAX_VALUE
                        5 -> Float.NEGATIVE_INFINITY
                        6 -> Float.POSITIVE_INFINITY
                        7 -> 0.0f
                        8 -> Float.NaN
                        else -> random.nextFloat().let { if (negate(choice)) -it else it }
                    }
                    KafkaType.FLOAT64 -> when (choice) {
                        1 -> Double.MIN_VALUE
                        2 -> -Double.MIN_VALUE
                        3 -> Double.MAX_VALUE
                        4 -> -Double.MAX_VALUE
                        5 -> Double.NEGATIVE_INFINITY
                        6 -> Double.POSITIVE_INFINITY
                        7 -> 0.0
                        8 -> Double.NaN
                        else -> random.nextDouble().let { if (negate(choice)) -it else it }
                    }
                    KafkaType.BOOLEAN -> choice < 10
                    KafkaType.STRING -> randomString(random)
                    KafkaType.BYTES -> {
                        val size = random.nextInt(50)
                        return ByteArray(size).also { random.nextBytes(it) }
                    }
                    KafkaType.ARRAY -> list(schema)
                    KafkaType.MAP -> map(schema)
                    KafkaType.STRUCT -> struct(schema)
                }
            }
        }
    }
}
