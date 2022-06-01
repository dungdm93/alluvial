package dev.alluvial.metric

import dev.alluvial.aws.metric.HttpClientMetricCollector
import dev.alluvial.aws.metric.MicrometerMetricPublisher
import dev.alluvial.aws.metric.ServiceClientMetricCollector
import dev.alluvial.metric.exporters.MetricExporter
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

class MetricService(
    val registry: CompositeMeterRegistry,
    config: MetricConfig
) : Runnable, Closeable {
    private val exporters : List<MetricExporter>

    init {
        exporters = config.exporters.map {
            MetricExporter.create(it.kind, it.properties)
                .also { exporter -> registry.add(exporter.registry) }
        }
        val tags = config.commonTags.map { Tag.of(it.key, it.value) }
        registry.config().commonTags(tags)
    }

    fun bindJvmMetrics(): MetricService = this.also {
        JvmMemoryMetrics().bindTo(registry)
        JvmThreadMetrics().bindTo(registry)
        JvmGcMetrics().bindTo(registry)
        JvmHeapPressureMetrics().bindTo(registry)
    }

    fun bindSystemMetrics(): MetricService = this.also {
        ProcessorMetrics().bindTo(registry)
        FileDescriptorMetrics().bindTo(registry)
    }

    fun bindAwsClientMetrics(): MetricService = this.also {
        MicrometerMetricPublisher.registerCollector(
            ServiceClientMetricCollector("S3", registry, Tags.empty())
        )
        MicrometerMetricPublisher.registerCollector(
            HttpClientMetricCollector(registry, Tags.empty())
        )
    }

    override fun run() {
        exporters.forEach(MetricExporter::run)
    }

    override fun close() {
        registry.close()
        exporters.forEach(MetricExporter::close)
    }
}
