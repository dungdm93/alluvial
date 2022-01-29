package dev.alluvial.sink.iceberg.data

import org.apache.iceberg.types.Types
import org.apache.kafka.connect.data.Decimal
import org.apache.iceberg.types.Type as IcebergType
import org.apache.kafka.connect.data.Field as KafkaField
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Schema.Type as KafkaType

/**
 * @see org.apache.iceberg.flink.FlinkTypeToType
 * @see org.apache.iceberg.spark.SparkTypeToType
 * @see io.confluent.connect.avro.AvroData
 */
class KafkaTypeToType : KafkaTypeVisitor<IcebergType>() {
    private var nextId: Int = 1

    private fun getNextId(): Int {
        return nextId++
    }

    override fun struct(schema: KafkaSchema, fieldResults: List<IcebergType>): IcebergType {
        val fields = schema.fields()
        val icebergFields = buildList(fields.size) {
            fields.forEachIndexed { idx, field ->
                val fieldId = getNextId()
                val fieldSchema = fieldResults[idx]
                val icebergField = Types.NestedField.of(
                    fieldId,
                    field.schema().isOptional,
                    field.name(),
                    fieldSchema,
                    field.schema().doc()
                )
                add(icebergField)
            }
        }
        return Types.StructType.of(icebergFields)
    }

    override fun field(field: KafkaField, fieldSchema: IcebergType): IcebergType {
        return fieldSchema
    }

    override fun map(schema: KafkaSchema, keyResult: IcebergType, valueResult: IcebergType): IcebergType {
        val keyId = getNextId()
        val valueId = getNextId()
        return if (schema.valueSchema().isOptional)
            Types.MapType.ofOptional(keyId, valueId, keyResult, valueResult) else
            Types.MapType.ofRequired(keyId, valueId, keyResult, valueResult)
    }

    override fun array(schema: KafkaSchema, elementResult: IcebergType): IcebergType {
        val elementId = getNextId()
        return if (schema.valueSchema().isOptional)
            Types.ListType.ofOptional(elementId, elementResult) else
            Types.ListType.ofRequired(elementId, elementResult)
    }

    override fun primitive(schema: KafkaSchema): IcebergType {
        return when (schema.name()) {
            "org.apache.kafka.connect.data.Date" -> Types.DateType.get()
            "org.apache.kafka.connect.data.Time" -> Types.TimeType.get()
            "org.apache.kafka.connect.data.Timestamp" -> Types.TimestampType.withoutZone()
            "org.apache.kafka.connect.data.Decimal" -> {
                val params = schema.parameters()
                val precision = (params["precision"] ?: params["connect.decimal.precision"] ?: "38").toInt()
                val scale = params.getOrDefault(Decimal.SCALE_FIELD, "10").toInt()
                Types.DecimalType.of(precision, scale)
            }
            //    "io.debezium.data.Bits"
            //    "io.debezium.time.Date"
            //    "io.debezium.time.Time"
            //    "io.debezium.time.MicroTime"
            //    "io.debezium.time.NanoTime"
            //    "io.debezium.time.ZonedTime"
            //    "io.debezium.time.Timestamp"
            //    "io.debezium.time.MicroTimestamp"
            //    "io.debezium.time.NanoTimestamp"
            //    "io.debezium.time.ZonedTimestamp"
            //    "io.debezium.time.MicroDuration"
            //    "io.debezium.time.NanoDuration"
            //    "io.debezium.time.Interval"
            //    "io.debezium.time.Year"
            //    "io.debezium.data.Json"
            //    "io.debezium.data.Xml"
            //    "io.debezium.data.Uuid"
            //    "io.debezium.data.geometry.Point"
            //    "io.debezium.data.geometry.Geometry"
            //    "io.debezium.data.geometry.Geography"
            //    "io.debezium.data.Ltree"
            //    "io.debezium.data.Enum"
            //    "io.debezium.data.EnumSet"
            //    "io.debezium.data.VariableScaleDecimal"
            else -> when (schema.type()) {
                KafkaType.INT8,
                KafkaType.INT16,
                KafkaType.INT32 -> Types.IntegerType.get()
                KafkaType.INT64 -> Types.LongType.get()
                KafkaType.FLOAT32 -> Types.FloatType.get()
                KafkaType.FLOAT64 -> Types.DoubleType.get()
                KafkaType.BOOLEAN -> Types.BooleanType.get()
                KafkaType.STRING -> Types.StringType.get()
                KafkaType.BYTES -> Types.BinaryType.get()
                else -> throw IllegalArgumentException("$schema is not primitive")
            }
        }
    }
}
