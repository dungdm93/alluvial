package dev.alluvial.sink.iceberg.type.logical

import dev.alluvial.sink.iceberg.type.KafkaSchema
import java.util.ServiceLoader

private var logicalTypeProviderLoader = ServiceLoader.load(LogicalTypeProvider::class.java)

fun KafkaSchema.logicalTypeConverter(): LogicalTypeConverter<*, *>? {
    val logicalName = this.name() ?: return null
    logicalTypeProviderLoader.forEach { provider ->
        val converter = provider.getConverter(logicalName)
        if (converter != null) return converter
    }
    return null
}
