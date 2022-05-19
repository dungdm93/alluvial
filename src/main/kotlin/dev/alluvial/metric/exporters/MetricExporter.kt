package dev.alluvial.metric.exporters

import io.micrometer.core.instrument.MeterRegistry
import java.io.Closeable

abstract class MetricExporter : Runnable, Closeable {
    abstract val registry: MeterRegistry

    companion object Factory {
        fun create(kind: String, properties: Map<String, String>): MetricExporter {
            return when (kind) {
                "prometheus" -> PrometheusMetricExporter(properties)
                // Add new provider here (Elasticsearch, Datadog, etc)
                else -> throw UnsupportedOperationException("Meter registry \"${kind}\" not supported")
            }
        }
    }
}
