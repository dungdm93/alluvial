package dev.alluvial.sink.iceberg.data

import org.apache.iceberg.types.Type.TypeID.*
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.types.Type as IcebergType
import org.apache.kafka.connect.data.Schema as KafkaSchema

@Suppress("MemberVisibilityCanBePrivate")
object KafkaSchemaUtil {
    /**
     * Convert an [IcebergSchema] to a [KafkaSchema].
     *
     * @param icebergSchema an Iceberg Schema
     * @return the equivalent Kafka Schema
     * @throws IllegalArgumentException if the type cannot be converted to Kafka
     */
    fun toKafkaSchema(icebergSchema: IcebergSchema): KafkaSchema {
        return TypeUtil.visit(icebergSchema, TypeToKafkaType())
    }

    /**
     * Convert an [IcebergType] to a [KafkaSchema].
     *
     * @param icebergType an Iceberg Type
     * @return the equivalent Kafka Schema
     * @throws IllegalArgumentException if the type cannot be converted to Kafka
     */
    fun toKafkaSchema(icebergType: IcebergType): KafkaSchema {
        return TypeUtil.visit(icebergType, TypeToKafkaType())
    }

    /**
     * Convert a [KafkaSchema] to an [IcebergSchema].
     *
     * @param kafkaSchema a Kafka Schema
     * @return the equivalent Iceberg Schema
     * @throws IllegalArgumentException if the type cannot be converted to Iceberg
     */
    fun toIcebergSchema(kafkaSchema: KafkaSchema): IcebergSchema {
        val converted = KafkaTypeToType().visit(kafkaSchema)
        return IcebergSchema(converted.asStructType().fields())
    }

    /**
     * Convert a [KafkaSchema] to an [IcebergSchema].
     *
     * @param kafkaSchema a Kafka Schema
     * @return the equivalent Iceberg Schema
     * @throws IllegalArgumentException if the type cannot be converted to Iceberg
     */
    fun toIcebergSchema(kafkaSchema: KafkaSchema, keys: List<String>): IcebergSchema {
        val converted = KafkaTypeToType().visit(kafkaSchema)
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
    fun toIcebergType(kafkaSchema: KafkaSchema): IcebergType {
        return KafkaTypeToType().visit(kafkaSchema)
    }
}
