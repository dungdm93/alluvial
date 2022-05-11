package dev.alluvial.sink.iceberg.io

import dev.alluvial.sink.iceberg.type.IcebergField
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.IcebergType
import dev.alluvial.sink.iceberg.type.KafkaField
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaStruct
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.logicalTypeConverter
import org.apache.iceberg.StructLike
import org.apache.iceberg.types.Type.TypeID.*
import org.apache.iceberg.types.Types
import java.nio.ByteBuffer

class StructWrapper(sSchema: KafkaSchema, iStruct: Types.StructType) : StructLike {
    private val getters: List<Getter<*>> = iStruct.fields().map { iField ->
        val sField = sSchema.field(iField.name())
        getter(sField, iField)
    }
    private var wrapped: KafkaStruct? = null

    constructor(sSchema: KafkaSchema, iSchema: IcebergSchema) : this(sSchema, iSchema.asStruct())

    fun wrap(data: KafkaStruct): StructWrapper {
        this.wrapped = data
        return this
    }

    override fun size(): Int {
        return getters.size
    }

    override fun <T> get(pos: Int, javaClass: Class<T>): T? {
        val getter = getters[pos]
        val value = getter(wrapped!!)
        return javaClass.cast(value)
    }

    override fun <T> set(pos: Int, value: T?) {
        throw UnsupportedOperationException("StructWrapper is read-only")
    }

    private fun getter(sField: KafkaField, iField: IcebergField): Getter<*> {
        val fieldName = iField.name()
        val fieldType = iField.type()
        val sSchema = sField.schema()

        val logicalTypeConverter = sSchema.logicalTypeConverter()
        val getter = if (logicalTypeConverter != null)
            logicalTypeGetter(fieldName, logicalTypeConverter) else
            standardGetter(fieldName, sSchema, fieldType)

        return if (sSchema.isOptional)
            nullable(fieldName, getter) else
            getter
    }

    private fun <T> nullable(fieldName: String, getter: Getter<T>): Getter<T?> {
        return g@{ struct ->
            struct[fieldName] ?: return@g null
            getter(struct)
        }
    }

    private fun standardGetter(
        fieldName: String,
        sSchema: KafkaSchema,
        fieldType: IcebergType,
    ): Getter<*> {
        return when (fieldType.typeId()) {
            INTEGER -> intGetter(fieldName)
            FIXED,
            BINARY -> binaryGetter(fieldName)
            STRUCT -> structGetter(fieldName, sSchema, fieldType)
            else -> { struct -> struct[fieldName] }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <K, I> logicalTypeGetter(fieldName: String, converter: LogicalTypeConverter<K, I>): Getter<I> {
        return { struct ->
            val value = struct[fieldName] as K
            converter.toIcebergValue(value)
        }
    }

    private fun intGetter(fieldName: String): Getter<Int> {
        return { struct ->
            val value = struct[fieldName] as Number
            value.toInt()
        }
    }

    private fun binaryGetter(fieldName: String): Getter<*> {
        return { struct ->
            when (val value = struct[fieldName]) {
                is ByteArray -> ByteBuffer.wrap(value)
                is ByteBuffer -> value
                else -> throw UnsupportedOperationException(
                    "Wrapper does not support binary type ${value.javaClass.name}"
                )
            }
        }
    }

    private fun structGetter(
        fieldName: String,
        sSchema: KafkaSchema,
        fieldType: IcebergType
    ): Getter<StructWrapper> {
        val nestedWrapper = StructWrapper(sSchema, fieldType.asStructType())
        return { struct ->
            val nestedStruct = struct[fieldName] as KafkaStruct
            nestedWrapper.wrap(nestedStruct)
        }
    }
}
