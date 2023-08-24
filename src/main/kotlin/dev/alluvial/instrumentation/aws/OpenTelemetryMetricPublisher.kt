package dev.alluvial.instrumentation.aws

import software.amazon.awssdk.metrics.MetricCollection
import software.amazon.awssdk.metrics.MetricPublisher

class OpenTelemetryMetricPublisher : MetricPublisher {
    override fun publish(metricCollection: MetricCollection?) {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}
