package dev.alluvial.runtime

import dev.alluvial.utils.SERVICE_COMPONENT
import dev.alluvial.utils.Units.BUFFERS
import dev.alluvial.utils.Units.BYTES
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes
import org.slf4j.bridge.SLF4JBridgeHandler
import java.lang.management.BufferPoolMXBean
import java.lang.management.ManagementFactory
import java.lang.management.MemoryPoolMXBean
import java.lang.management.MemoryUsage

abstract class Instrumental {
    companion object {
        const val INSTRUMENTATION_NAME = "dev.alluvial"
        const val INSTRUMENTATION_VERSION = "0.13.0"
        private val OPENTELEMETRY_DEFAULT_PROPERTIES = mapOf(
            "otel.traces.exporter" to "none",
            "otel.metrics.exporter" to "none",
            "otel.logs.exporter" to "none",
        )
    }

    protected lateinit var telemetry: OpenTelemetry
    protected lateinit var tracer: Tracer
    protected lateinit var meter: Meter

    init {
        SLF4JBridgeHandler.removeHandlersForRootLogger()
        SLF4JBridgeHandler.install()
    }

    protected fun initializeTelemetry(config: Config, component: String) {
        telemetry = if (!config.telemetry.enabled)
            OpenTelemetry.noop() else
            AutoConfiguredOpenTelemetrySdk.builder()
                .addPropertiesSupplier { OPENTELEMETRY_DEFAULT_PROPERTIES + config.telemetry.properties }
                .addResourceCustomizer { r, _ ->
                    val rb = r.toBuilder()
                    rb.put(ResourceAttributes.SERVICE_NAME, "alluvial")
                    rb.put(ResourceAttributes.SERVICE_VERSION, INSTRUMENTATION_VERSION)
                    rb.put(SERVICE_COMPONENT, component)
                    rb.build()
                }
                .setResultAsGlobal()
                .build()
                .openTelemetrySdk
        tracer = telemetry.tracerBuilder(INSTRUMENTATION_NAME)
            .setInstrumentationVersion(INSTRUMENTATION_VERSION)
            .build()
        meter = telemetry.meterBuilder(INSTRUMENTATION_NAME)
            .setInstrumentationVersion(INSTRUMENTATION_VERSION)
            .build()

        instrumentJvmMemoryMetrics()
    }

    /**
     * @see io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
     */
    private fun instrumentJvmMemoryMetrics() {
        ManagementFactory.getPlatformMXBeans(BufferPoolMXBean::class.java).forEach { bean ->
            val attrs = Attributes.of(stringKey("id"), bean.name)

            meter.gaugeBuilder("jvm.buffer.count").ofLongs()
                .setDescription("An estimate of the number of buffers in the pool")
                .setUnit(BUFFERS)
                .buildWithCallback { it.record(bean.count, attrs) }
            meter.gaugeBuilder("jvm.buffer.memory.used").ofLongs()
                .setDescription("An estimate of the memory that the Java virtual machine is using for this buffer pool")
                .setUnit(BYTES)
                .buildWithCallback { it.record(bean.memoryUsed, attrs) }
            meter.gaugeBuilder("jvm.buffer.total.capacity").ofLongs()
                .setDescription("An estimate of the total capacity of the buffers in this pool")
                .setUnit(BYTES)
                .buildWithCallback { it.record(bean.totalCapacity, attrs) }
        }

        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean::class.java).forEach { bean ->
            val attrs = Attributes.of(
                stringKey("id"), bean.name,
                stringKey("area"), bean.type.name.lowercase()
            )

            meter.gaugeBuilder("jvm.memory.used").ofLongs()
                .setDescription("The amount of used memory")
                .setUnit(BYTES)
                .buildWithCallback {
                    val value = getUsageValue(bean, MemoryUsage::getUsed)
                    it.record(value, attrs)
                }

            meter.gaugeBuilder("jvm.memory.committed").ofLongs()
                .setDescription("The amount of memory in bytes that is committed for the Java virtual machine to use")
                .setUnit(BYTES)
                .buildWithCallback {
                    val value = getUsageValue(bean, MemoryUsage::getCommitted)
                    it.record(value, attrs)
                }

            meter.gaugeBuilder("jvm.memory.max").ofLongs()
                .setDescription("The maximum amount of memory in bytes that can be used for memory management")
                .setUnit(BYTES)
                .buildWithCallback {
                    val value = getUsageValue(bean, MemoryUsage::getMax)
                    it.record(value, attrs)
                }
        }
    }

    private fun getUsageValue(bean: MemoryPoolMXBean, getter: (MemoryUsage) -> Long): Long {
        try {
            val usage = bean.usage ?: return 0
            return getter(usage)
        } catch (e: InternalError) {
            return 0
        }
    }
}
