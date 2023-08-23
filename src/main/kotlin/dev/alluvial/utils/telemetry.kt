package dev.alluvial.utils

import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer

val SERVICE_COMPONENT = stringKey("service.component")

/**
 * @see io.micrometer.core.instrument.binder.BaseUnits
 */
object Units {
    const val BYTES = "bytes"
    const val ROWS = "rows"
    const val TASKS = "tasks"
    const val THREADS = "threads"
    const val CLASSES = "classes"
    const val BUFFERS = "buffers"
    const val EVENTS = "events"
    const val FILES = "files"
    const val SESSIONS = "sessions"
    const val MILLISECONDS = "ms"
    const val MESSAGES = "messages"
    const val CONNECTIONS = "connections"
    const val OPERATIONS = "operations"
    const val PERCENT = "percent"
    const val OBJECTS = "objects"
}

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
