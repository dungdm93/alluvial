package dev.alluvial.metrics.exporters

import io.micrometer.core.instrument.MeterRegistry
import java.io.Closeable

abstract class MetricsExporter : Runnable, Closeable {
    abstract val registry: MeterRegistry

    companion object Factory {
        fun create(kind: String, properties: Map<String, String>): MetricsExporter {
            return when (kind) {
                "prometheus" -> PrometheusMetricsExporter(properties)
                // Add new provider here (Elasticsearch, Datadog, etc)
                else -> throw UnsupportedOperationException("MeterRegistry \"${kind}\" not supported")
            }
        }
    }
}
