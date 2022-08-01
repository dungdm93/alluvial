package dev.alluvial.metrics

import dev.alluvial.sink.iceberg.aws.HttpClientMetricCollector
import dev.alluvial.sink.iceberg.aws.MicrometerMetricPublisher
import dev.alluvial.sink.iceberg.aws.ServiceClientMetricCollector
import dev.alluvial.metrics.exporters.MetricsExporter
import dev.alluvial.runtime.MetricConfig
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import java.io.Closeable

class MetricsService(
    private val registry: CompositeMeterRegistry,
    config: MetricConfig
) : Runnable, Closeable {
    private val exporters: List<MetricsExporter>

    init {
        exporters = config.exporters.map {
            val exporter = MetricsExporter.create(it.kind, it.properties)
            registry.add(exporter.registry)
            exporter
        }
        val tags = config.commonTags.map { Tag.of(it.key, it.value) }
        registry.config().commonTags(tags)
    }

    fun bindJvmMetrics(): MetricsService {
        JvmMemoryMetrics().bindTo(registry)
        JvmThreadMetrics().bindTo(registry)
        JvmGcMetrics().bindTo(registry)
        JvmHeapPressureMetrics().bindTo(registry)
        return this
    }

    fun bindSystemMetrics(): MetricsService {
        ProcessorMetrics().bindTo(registry)
        FileDescriptorMetrics().bindTo(registry)
        return this
    }

    fun bindAwsClientMetrics(): MetricsService {
        MicrometerMetricPublisher.registerCollector(
            ServiceClientMetricCollector("S3", registry, Tags.empty())
        )
        MicrometerMetricPublisher.registerCollector(
            HttpClientMetricCollector(registry, Tags.empty())
        )
        return this
    }

    override fun run() {
        exporters.forEach(MetricsExporter::run)
    }

    override fun close() {
        exporters.forEach(MetricsExporter::close)
        registry.close()
    }
}
