package dev.alluvial.sink.iceberg.io

import org.apache.iceberg.StructLike
import org.apache.iceberg.types.Type.TypeID.*
import org.apache.iceberg.types.Types
import java.time.ZoneOffset
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct
import java.util.Date as JDate

class StructWrapper(iStruct: Types.StructType) : StructLike {
    private val getters: List<Getter> = iStruct.fields().map(::getter)
    private var wrapped: KafkaStruct? = null

    constructor(iSchema: IcebergSchema) : this(iSchema.asStruct())

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

    private fun getter(field: Types.NestedField): Getter {
        val fieldName = field.name()
        val fieldType = field.type()
        return when (fieldType.typeId()) {
            STRUCT -> {
                val nestedWrapper = StructWrapper(fieldType.asStructType())
                return { struct ->
                    val nestedStruct = struct[fieldName]
                    nestedStruct?.let { nestedWrapper.wrap(it as KafkaStruct) }
                }
            }
            DATE -> { struct ->
                struct[fieldName]?.let {
                    (it as JDate).toInstant()
                        .atZone(ZoneOffset.UTC)
                        .toLocalDate()
                }
            }
            TIME -> { struct ->
                struct[fieldName]?.let {
                    (it as JDate).toInstant()
                        .atZone(ZoneOffset.UTC)
                        .toLocalTime()
                }
            }
            TIMESTAMP -> { struct ->
                struct[fieldName]?.let {
                    (it as JDate).toInstant()
                        .atZone(ZoneOffset.UTC)
                        .toLocalDateTime()
                }
            }
            else -> { struct -> struct[fieldName] }
        }
    }
}
