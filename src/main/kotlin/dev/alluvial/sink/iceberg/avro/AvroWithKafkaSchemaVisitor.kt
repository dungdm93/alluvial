package dev.alluvial.sink.iceberg.avro

import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaType
import org.apache.iceberg.avro.AvroWithPartnerByStructureVisitor
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.util.Pair

abstract class AvroWithKafkaSchemaVisitor<T> : AvroWithPartnerByStructureVisitor<KafkaSchema, T>() {
    override fun isStringType(type: KafkaSchema) = type.type() == KafkaType.STRING

    override fun isMapType(type: KafkaSchema) = type.type() == KafkaType.MAP

    override fun mapKeyType(mapType: KafkaSchema): KafkaSchema {
        Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType)
        return mapType.keySchema()
    }

    override fun mapValueType(mapType: KafkaSchema): KafkaSchema {
        Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType)
        return mapType.valueSchema()
    }

    open fun isArrayType(type: KafkaSchema) = type.type() == KafkaType.ARRAY

    override fun arrayElementType(arrayType: KafkaSchema): KafkaSchema {
        return when (arrayType.name()) {
            io.debezium.data.EnumSet.LOGICAL_NAME -> arrayType
            else -> {
                Preconditions.checkArgument(isArrayType(arrayType), "Invalid array: %s is not an array", arrayType)
                return arrayType.valueSchema()
            }
        }
    }

    override fun fieldNameAndType(structType: KafkaSchema, pos: Int): Pair<String, KafkaSchema> {
        val field = structType.fields()[pos]
        return Pair.of(field.name(), field.schema())
    }

    override fun nullType(): KafkaSchema? = null
}
