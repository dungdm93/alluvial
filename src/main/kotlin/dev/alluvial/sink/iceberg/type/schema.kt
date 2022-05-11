package dev.alluvial.sink.iceberg.type

import org.apache.iceberg.types.Type.TypeID.*
import org.apache.iceberg.types.TypeUtil

/**
 * Convert an [IcebergSchema] to a [KafkaSchema].
 *
 * @param iSchema an Iceberg Schema
 * @return the equivalent Kafka Schema
 * @throws IllegalArgumentException if the type cannot be converted to Kafka
 */
fun IcebergSchema.toKafkaSchema(): KafkaSchema {
    return TypeUtil.visit(this, TypeToKafkaType())
}

/**
 * Convert an [IcebergType] to a [KafkaSchema].
 *
 * @param icebergType an Iceberg Type
 * @return the equivalent Kafka Schema
 * @throws IllegalArgumentException if the type cannot be converted to Kafka
 */
fun IcebergType.toKafkaSchema(): KafkaSchema {
    return TypeUtil.visit(this, TypeToKafkaType())
}

/**
 * Convert a [KafkaSchema] to an [IcebergSchema].
 *
 * @param kafkaSchema a Kafka Schema
 * @return the equivalent Iceberg Schema
 * @throws IllegalArgumentException if the type cannot be converted to Iceberg
 */
fun KafkaSchema.toIcebergSchema(): IcebergSchema {
    val converted = KafkaTypeToType().visit(this)
    return IcebergSchema(converted.asStructType().fields())
}

/**
 * Convert a [KafkaSchema] to an [IcebergSchema].
 *
 * @param kafkaSchema a Kafka Schema
 * @return the equivalent Iceberg Schema
 * @throws IllegalArgumentException if the type cannot be converted to Iceberg
 */
fun KafkaSchema.toIcebergSchema(keys: List<String>): IcebergSchema {
    val converted = KafkaTypeToType().visit(this)
    val struct = converted.asStructType()
    val identifierFieldIds = keys.map { struct.field(it).fieldId() }
    return IcebergSchema(struct.fields(), identifierFieldIds.toSet())
}

/**
 * Convert a [KafkaSchema] to an [IcebergType].
 *
 * @param kafkaSchema a Kafka Schema
 * @return the equivalent Iceberg Type
 * @throws IllegalArgumentException if the type cannot be converted to Kafka
 */
fun KafkaSchema.toIcebergType(): IcebergType {
    return KafkaTypeToType().visit(this)
}


private fun IcebergField.equalsIgnoreId(that: IcebergField): Boolean {
    // ignore compare id
    if (this.isOptional != that.isOptional) {
        return false
    } else if (this.name() != that.name()) {
        return false
    } else if (this.doc() != that.doc()) {
        return false
    }
    return this.type().equalsIgnoreId(that.type())
}

private fun IcebergType.equalsIgnoreId(that: IcebergType): Boolean {
    if (this.typeId() != that.typeId()) return false

    return when (this.typeId()) {
        LIST -> {
            val aList = this.asListType()
            val bList = that.asListType()
            return aList.isElementOptional == bList.isElementOptional
                && aList.elementType().equalsIgnoreId(bList.elementType())
        }
        MAP -> {
            val aMap = this.asMapType()
            val bMap = that.asMapType()
            return aMap.isValueOptional == bMap.isValueOptional
                && aMap.keyType().equalsIgnoreId(bMap.keyType())
                && aMap.valueType().equalsIgnoreId(bMap.valueType())
        }
        STRUCT -> {
            val aStruct = this.asStructType()
            val bStruct = that.asStructType()
            if (aStruct.fields().any { bStruct.field(it.name()) == null })
                return false
            if (bStruct.fields().any { aStruct.field(it.name()) == null })
                return false

            return aStruct.fields().all { aField ->
                val bField = bStruct.field(aField.name())
                aField.equalsIgnoreId(bField)
            }
        }
        else -> this == that
    }
}

fun KafkaField.isPromotionAllowed(iField: IcebergField): Boolean {
    return this.schema().isPromotionAllowed(iField.type())
}

fun KafkaSchema.isPromotionAllowed(iType: IcebergType): Boolean {
    return when (iType.typeId()) {
        STRUCT -> this.type() == KafkaType.STRUCT
        MAP -> {
            if (this.type() != KafkaType.MAP) return false
            if (!this.valueSchema().isPromotionAllowed(iType.asMapType().valueType())) return false
            // map key CAN'T be changed.
            // See org.apache.iceberg.SchemaUpdate.ApplyChanges.map
            val sKeyType = this.keySchema().toIcebergType()
            return sKeyType.equalsIgnoreId(iType.asMapType().keyType())
        }
        LIST -> this.type() == KafkaType.ARRAY &&
            this.valueSchema().isPromotionAllowed(iType.asListType().elementType())
        // Primitive type
        // Kafka's Schema has concept of logical type, e.g. org.apache.kafka.connect.data.Timestamp (INT64)
        // and io.debezium.time.Timestamp (STRING) both translate to Iceberg's TIMESTAMP.
        // So it's cannot static mapping between Kafka Schema type and Iceberg type.
        else -> {
            val eType = this.toIcebergType()
            eType.isPrimitiveType && TypeUtil.isPromotionAllowed(iType, eType.asPrimitiveType())
        }
    }
}

fun IcebergType.isPromotionAllowed(that: IcebergType): Boolean {
    return when (this.typeId()) {
        STRUCT -> that.typeId() == STRUCT
        MAP -> that.typeId() == MAP &&
            this.asMapType().keyType() == that.asMapType().keyType() &&
            this.asMapType().valueType().isPromotionAllowed(that.asMapType().valueType())
        LIST -> that.typeId() == LIST &&
            this.asListType().elementType().isPromotionAllowed(that.asListType().elementType())
        else -> that.isPrimitiveType &&
            TypeUtil.isPromotionAllowed(this, that.asPrimitiveType())
    }
}
