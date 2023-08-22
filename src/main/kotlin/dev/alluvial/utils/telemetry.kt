package dev.alluvial.utils

import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer

val SERVICE_COMPONENT = stringKey("service.component")

inline fun <R> Tracer.withSpan(
    name: String,
    attributes: Attributes = Attributes.empty(),
    block: (Span) -> R
): R {
    val span = this.spanBuilder(name)
        .setAllAttributes(attributes)
        .startSpan()
    val scope = span.makeCurrent()
    try {
        return block(span)
    } catch (t: Throwable) {
        span.setStatus(StatusCode.ERROR)
        span.recordException(t)
        throw t
    } finally {
        scope.close()
        span.end()
    }
}
