package dev.alluvial.sink.iceberg.type.logical

interface LogicalTypeProvider {
    val name: String

    fun getConverter(name: String): LogicalTypeConverter<*, *>?
}
