package dev.alluvial.utils

import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.Tracer

inline fun <R> Tracer.withSpan(name: String, block: (Span) -> R): R {
    val span = this.spanBuilder(name).startSpan()
    val scope = span.makeCurrent()
    try {
        return block(span)
    } finally {
        scope.close()
        span.end()
    }
}
