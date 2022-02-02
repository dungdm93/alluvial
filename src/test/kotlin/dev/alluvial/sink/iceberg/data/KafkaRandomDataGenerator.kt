package dev.alluvial.sink.iceberg.data

import org.apache.iceberg.relocated.com.google.common.collect.Sets
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.types.Types
import org.apache.iceberg.util.RandomUtil
import java.util.Date
import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Type.TypeID as IcebergType
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

internal object KafkaRandomDataGenerator {
    private fun dateFrom(days: Int): Date {
        val time = TimeUnit.DAYS.toMillis(days.toLong())
        return Date(time)
    }

    private fun datetimeFrom(timeMicros: Long): Date {
        var timeMillis = TimeUnit.MICROSECONDS.toMillis(timeMicros)
        // represent a timestamp before UNIX Epoch (1970-01-01T00:00:00+GMT)
        // then, timeMicros will be negative
        if (TimeUnit.MILLISECONDS.toMicros(timeMillis) > timeMicros) {
            timeMillis--
        }
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
}
