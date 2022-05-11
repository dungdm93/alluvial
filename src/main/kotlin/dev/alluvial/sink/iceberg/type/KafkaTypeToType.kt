package dev.alluvial.sink.iceberg.type

import dev.alluvial.sink.iceberg.type.logical.logicalTypeConverter
import org.apache.iceberg.types.Types.*
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
                val icebergField = NestedField.of(
                    fieldId,
                    field.schema().isOptional,
                    field.name(),
                    fieldSchema,
                    field.schema().doc()
                )
                add(icebergField)
            }
        }
        return StructType.of(icebergFields)
    }

    override fun field(field: KafkaField, fieldSchema: IcebergType): IcebergType {
        return fieldSchema
    }

    override fun map(schema: KafkaSchema, keyResult: IcebergType, valueResult: IcebergType): IcebergType {
        val keyId = getNextId()
        val valueId = getNextId()
        return if (schema.valueSchema().isOptional)
            MapType.ofOptional(keyId, valueId, keyResult, valueResult) else
            MapType.ofRequired(keyId, valueId, keyResult, valueResult)
    }

    override fun array(schema: KafkaSchema, elementResult: IcebergType): IcebergType {
        val elementId = getNextId()
        return if (schema.valueSchema().isOptional)
            ListType.ofOptional(elementId, elementResult) else
            ListType.ofRequired(elementId, elementResult)
    }

    override fun primitive(schema: KafkaSchema): IcebergType {
        val converter = schema.logicalTypeConverter()
        if (converter != null) {
            return converter.toIcebergType(this::getNextId, schema)
        }

        return when (schema.type()) {
            KafkaType.INT8,
            KafkaType.INT16,
            KafkaType.INT32 -> IntegerType.get()
            KafkaType.INT64 -> LongType.get()
            KafkaType.FLOAT32 -> FloatType.get()
            KafkaType.FLOAT64 -> DoubleType.get()
            KafkaType.BOOLEAN -> BooleanType.get()
            KafkaType.STRING -> StringType.get()
            KafkaType.BYTES -> BinaryType.get()
            else -> throw IllegalArgumentException("$schema is not primitive")
        }
    }
}
