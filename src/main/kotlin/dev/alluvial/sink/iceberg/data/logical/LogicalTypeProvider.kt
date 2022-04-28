package dev.alluvial.sink.iceberg.data.logical

interface LogicalTypeProvider {
    val name: String

    fun getConverter(name: String): LogicalTypeConverter<*, *>?
}
