package dev.alluvial.sink.iceberg.data.logical.kafka

import dev.alluvial.sink.iceberg.data.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.data.logical.LogicalTypeProvider

class LogicalTypeProvider : LogicalTypeProvider {
    private val converters = mapOf<String, LogicalTypeConverter<*, *>>(
        DecimalConverter.name to DecimalConverter,
        DateConverter.name to DateConverter,
        TimeConverter.name to TimeConverter,
        TimestampConverter.name to TimestampConverter,
    )

    override val name: String = "kafka"

    override fun getConverter(name: String): LogicalTypeConverter<*, *>? {
        return converters[name]
    }
}
