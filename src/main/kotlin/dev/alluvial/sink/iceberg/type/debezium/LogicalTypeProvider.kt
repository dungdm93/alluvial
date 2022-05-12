package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeProvider

class LogicalTypeProvider : LogicalTypeProvider {
    private val converters = mapOf<String, LogicalTypeConverter<*, *>>(
        DateConverter.name to DateConverter,
        TimeConverter.name to TimeConverter,
        MicroTimeConverter.name to MicroTimeConverter,
        NanoTimeConverter.name to NanoTimeConverter,
        ZonedTimeConverter.name to ZonedTimeConverter,
        TimestampConverter.name to TimestampConverter,
        MicroTimestampConverter.name to MicroTimestampConverter,
        NanoTimestampConverter.name to NanoTimestampConverter,
        ZonedTimestampConverter.name to ZonedTimestampConverter,
        YearConverter.name to YearConverter,
        EnumConverter.name to EnumConverter,
        // EnumSetConverter.name to EnumSetConverter,
        // GeometryConverter.name to GeometryConverter,
    )

    override val name: String = "debezium"

    override fun getConverter(name: String): LogicalTypeConverter<*, *>? {
        return converters[name]
    }
}
