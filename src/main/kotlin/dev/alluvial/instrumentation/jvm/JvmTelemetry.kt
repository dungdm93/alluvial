package dev.alluvial.instrumentation.jvm

import dev.alluvial.utils.Units
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.lang.management.BufferPoolMXBean
import java.lang.management.ManagementFactory
import java.lang.management.MemoryPoolMXBean
import java.lang.management.MemoryUsage

class JvmTelemetry(
    telemetry: OpenTelemetry
) {
    companion object {
        const val INSTRUMENTATION_NAME = "dev.alluvial"
        const val INSTRUMENTATION_VERSION = "0.13.0"
    }

    private val meter = telemetry.meterBuilder(INSTRUMENTATION_NAME)
        .setInstrumentationVersion(INSTRUMENTATION_VERSION)
        .build()

    /**
     * @see io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
     */
    fun registerMemoryMetrics() {
        ManagementFactory.getPlatformMXBeans(BufferPoolMXBean::class.java).forEach { bean ->
            val attrs = Attributes.of(AttributeKey.stringKey("id"), bean.name)

            meter.gaugeBuilder("jvm.buffer.count").ofLongs()
                .setDescription("An estimate of the number of buffers in the pool")
                .setUnit(Units.BUFFERS)
                .buildWithCallback { it.record(bean.count, attrs) }
            meter.gaugeBuilder("jvm.buffer.memory.used").ofLongs()
                .setDescription("An estimate of the memory that the Java virtual machine is using for this buffer pool")
                .setUnit(Units.BYTES)
                .buildWithCallback { it.record(bean.memoryUsed, attrs) }
            meter.gaugeBuilder("jvm.buffer.total.capacity").ofLongs()
                .setDescription("An estimate of the total capacity of the buffers in this pool")
                .setUnit(Units.BYTES)
                .buildWithCallback { it.record(bean.totalCapacity, attrs) }
        }

        ManagementFactory.getPlatformMXBeans(MemoryPoolMXBean::class.java).forEach { bean ->
            val attrs = Attributes.of(
                AttributeKey.stringKey("id"), bean.name,
                AttributeKey.stringKey("area"), bean.type.name.lowercase()
            )

            meter.gaugeBuilder("jvm.memory.used").ofLongs()
                .setDescription("The amount of used memory")
                .setUnit(Units.BYTES)
                .buildWithCallback {
                    val value = getUsageValue(bean, MemoryUsage::getUsed)
                    it.record(value, attrs)
                }

            meter.gaugeBuilder("jvm.memory.committed").ofLongs()
                .setDescription("The amount of memory in bytes that is committed for the Java virtual machine to use")
                .setUnit(Units.BYTES)
                .buildWithCallback {
                    val value = getUsageValue(bean, MemoryUsage::getCommitted)
                    it.record(value, attrs)
                }

            meter.gaugeBuilder("jvm.memory.max").ofLongs()
                .setDescription("The maximum amount of memory in bytes that can be used for memory management")
                .setUnit(Units.BYTES)
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
