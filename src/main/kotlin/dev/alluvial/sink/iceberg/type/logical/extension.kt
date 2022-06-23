package dev.alluvial.sink.iceberg.type.logical

import dev.alluvial.sink.iceberg.type.KafkaSchema
import java.util.ServiceLoader

/**
 * Since ServiceLoader is not thread-safe, preload all providers to avoid error
 * @see java.util.ServiceLoader
 */
private val logicalTypeProviders = ServiceLoader.load(LogicalTypeProvider::class.java).toList()

fun KafkaSchema.logicalTypeConverter(): LogicalTypeConverter<*, *>? {
    val logicalName = this.name() ?: return null
    logicalTypeProviders.forEach { provider ->
        val converter = provider.getConverter(logicalName)
        if (converter != null) return converter
    }
    return null
}
