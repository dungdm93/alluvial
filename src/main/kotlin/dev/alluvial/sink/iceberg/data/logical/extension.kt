package dev.alluvial.sink.iceberg.data.logical

import java.util.ServiceLoader
import org.apache.kafka.connect.data.Schema as KafkaSchema

private var logicalTypeProviderLoader = ServiceLoader.load(LogicalTypeProvider::class.java)

fun KafkaSchema.logicalTypeConverter(): LogicalTypeConverter<*, *>? {
    val logicalName = this.name() ?: return null
    logicalTypeProviderLoader.forEach { provider ->
        val converter = provider.getConverter(logicalName)
        if (converter != null) return converter
    }
    return null
}
