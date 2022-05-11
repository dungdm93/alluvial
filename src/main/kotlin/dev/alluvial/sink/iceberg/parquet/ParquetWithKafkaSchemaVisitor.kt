package dev.alluvial.sink.iceberg.parquet

import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaType
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.util.Pair

abstract class ParquetWithKafkaSchemaVisitor<T> : ParquetWithPartnerByStructureVisitor<KafkaSchema, T>() {
    override fun isArrayType(type: KafkaSchema): Boolean = type.type() == KafkaType.ARRAY

    override fun arrayElementType(arrayType: KafkaSchema): KafkaSchema {
        Preconditions.checkArgument(isArrayType(arrayType), "Invalid array: %s is not an array", arrayType)
        return arrayType.valueSchema()
    }

    override fun isMapType(type: KafkaSchema): Boolean = type.type() == KafkaType.MAP

    override fun mapKeyType(mapType: KafkaSchema): KafkaSchema {
        Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType)
        return mapType.keySchema()
    }

    override fun mapValueType(mapType: KafkaSchema): KafkaSchema {
        Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType)
        return mapType.valueSchema()
    }

    override fun isStructType(type: KafkaSchema): Boolean = type.type() == KafkaType.STRUCT

    override fun fieldNameAndType(structType: KafkaSchema, pos: Int): Pair<String, KafkaSchema> {
        val field = structType.fields()[pos]
        return Pair.of(field.name(), field.schema())
    }

    override fun nullType(): KafkaSchema? = null
}
