package dev.alluvial.runtime

import dev.alluvial.utils.SERVICE_COMPONENT
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes

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
    }
}
