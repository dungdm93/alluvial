package dev.alluvial.sink.iceberg.type

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.types.Types.*
import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema.Type.*
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Schema.Type as KafkaType
import org.apache.kafka.connect.data.SchemaBuilder as KafkaSchemaBuilder

/**
 * @see org.apache.iceberg.flink.TypeToFlinkType
 * @see org.apache.iceberg.spark.TypeToSparkType
 */
class TypeToKafkaType : TypeUtil.SchemaVisitor<KafkaSchema>() {
    private data class KafkaSchemaInfo(
        var type: KafkaType,
        var optional: Boolean,
        var defaultValue: Any? = null,
        var name: String? = null,
        var version: Int? = null,
        var doc: String? = null,
        var parameters: Map<String, String>? = null,
        var fields: List<Field>? = null,
        var keySchema: KafkaSchema? = null,
        var valueSchema: KafkaSchema? = null,
    )

    companion object {
        private val WELL_KNOWN_SCHEMAS = listOf(
            KafkaSchema.INT8_SCHEMA, KafkaSchema.OPTIONAL_INT8_SCHEMA,
            KafkaSchema.INT16_SCHEMA, KafkaSchema.OPTIONAL_INT16_SCHEMA,
            KafkaSchema.INT32_SCHEMA, KafkaSchema.OPTIONAL_INT32_SCHEMA,
            KafkaSchema.INT64_SCHEMA, KafkaSchema.OPTIONAL_INT64_SCHEMA,
            KafkaSchema.FLOAT32_SCHEMA, KafkaSchema.OPTIONAL_FLOAT32_SCHEMA,
            KafkaSchema.FLOAT64_SCHEMA, KafkaSchema.OPTIONAL_FLOAT64_SCHEMA,
            KafkaSchema.BOOLEAN_SCHEMA, KafkaSchema.OPTIONAL_BOOLEAN_SCHEMA,
            KafkaSchema.STRING_SCHEMA, KafkaSchema.OPTIONAL_STRING_SCHEMA,
            KafkaSchema.BYTES_SCHEMA, KafkaSchema.OPTIONAL_BYTES_SCHEMA,
        )

        private fun KafkaSchema.clone(modifier: (KafkaSchemaInfo) -> Unit): KafkaSchema {
            val info = KafkaSchemaInfo(
                this.type(), this.isOptional, this.defaultValue(),
                this.name(), this.version(), this.doc(), this.parameters()
            )
            when (this.type()) {
                STRUCT -> info.fields = this.fields()
                MAP -> {
                    info.keySchema = this.keySchema()
                    info.valueSchema = this.valueSchema()
                }
                ARRAY -> info.valueSchema = this.valueSchema()
                else -> {}
            }
            modifier(info)
            val schema = ConnectSchema(
                info.type, info.optional, info.defaultValue,
                info.name, info.version, info.doc,
                info.parameters, info.fields, info.keySchema, info.valueSchema
            )
            for (s in WELL_KNOWN_SCHEMAS) {
                if (s == schema) return s
            }
            return schema
        }
    }

    override fun schema(schema: Schema, structResult: KafkaSchema): KafkaSchema {
        return structResult
    }

    override fun struct(struct: StructType, fieldResults: List<KafkaSchema>): KafkaSchema {
        val fields = struct.fields()

        val schemaBuilder = KafkaSchemaBuilder.struct()
        fields.forEachIndexed { idx, field ->
            schemaBuilder.field(field.name(), fieldResults[idx])
        }
        return schemaBuilder.build()
    }

    override fun field(field: NestedField, fieldResult: KafkaSchema): KafkaSchema {
        return if (field.isOptional != fieldResult.isOptional)
            fieldResult.clone { it.optional = field.isOptional } else
            fieldResult
    }

    override fun list(list: ListType, elementResult: KafkaSchema): KafkaSchema {
        val element = if (list.isElementOptional != elementResult.isOptional)
            elementResult.clone { it.optional = list.isElementOptional } else
            elementResult
        return KafkaSchemaBuilder.array(element).build()
    }

    override fun map(map: MapType, keyResult: KafkaSchema, valueResult: KafkaSchema): KafkaSchema {
        val value = if (map.isValueOptional != valueResult.isOptional)
            valueResult.clone { it.optional = map.isValueOptional } else
            valueResult
        return KafkaSchemaBuilder.map(keyResult, value).build()
    }

    override fun primitive(primitive: Type.PrimitiveType): KafkaSchema {
        // TODO: customize schema via *.handling.mode
        return when (primitive.typeId()) {
            TypeID.BOOLEAN -> KafkaSchema.BOOLEAN_SCHEMA
            TypeID.INTEGER -> KafkaSchema.INT32_SCHEMA
            TypeID.LONG -> KafkaSchema.INT64_SCHEMA
            TypeID.FLOAT -> KafkaSchema.FLOAT32_SCHEMA
            TypeID.DOUBLE -> KafkaSchema.FLOAT64_SCHEMA
            TypeID.DATE -> Date.SCHEMA
            TypeID.TIME -> Time.SCHEMA
            TypeID.TIMESTAMP -> Timestamp.SCHEMA
            TypeID.STRING -> KafkaSchema.STRING_SCHEMA
            TypeID.UUID -> KafkaSchema.STRING_SCHEMA
            TypeID.FIXED -> {
                val fixedType = primitive as FixedType
                KafkaSchemaBuilder.bytes()
                    .parameter("connect.fixed.size", fixedType.length().toString())
                    .build()
            }
            TypeID.BINARY -> KafkaSchema.BYTES_SCHEMA
            TypeID.DECIMAL -> {
                val decimalType = primitive as DecimalType
                Decimal.builder(decimalType.scale())
                    .parameter("connect.decimal.precision", decimalType.precision().toString())
                    .build()
            }
            else -> throw UnsupportedOperationException("Iceberg type $primitive is not primitive")
        }
    }
}
