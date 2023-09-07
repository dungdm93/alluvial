package dev.alluvial.instrumentation.aws

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongHistogram
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.metrics.CoreMetric.*
import software.amazon.awssdk.http.HttpMetric.HTTP_STATUS_CODE
import software.amazon.awssdk.metrics.MetricCollection
import software.amazon.awssdk.metrics.MetricPublisher
import java.util.ArrayDeque
import java.util.Deque

/**
 * @see software.amazon.awssdk.metrics.LoggingMetricPublisher
 * @see software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher
 */
class OpenTelemetryMetricPublisher(
    telemetry: OpenTelemetry
) : MetricPublisher {
    companion object {
        private val logger = LoggerFactory.getLogger(OpenTelemetryMetricPublisher::class.java)

        private val AWS_SERVICE = stringKey("service")
        private val AWS_OPERATION = stringKey("operation")
        private val HTTP_RESPONSE_STATUS_CODE = stringKey("code")
    }

    private val meter = telemetry.getMeter("io.opentelemetry.aws-sdk-2.2")
    private val apiCallRetries = mutableMapOf<String, LongCounter>()
    private val apiCallDurations = mutableMapOf<String, LongHistogram>()
    private val serviceCallDurations = meter.histogramBuilder("aws.http.service_call.duration").ofLongs()
        .setDescription("HTTP request duration to AWS services in ms")
        .setUnit("ms")
        .build()

    private fun getApiCallDurationHistogram(service: String): LongHistogram {
        return apiCallDurations.computeIfAbsent(service) {
            meter.histogramBuilder("aws.${service}.api_call.duration").ofLongs()
                .setDescription("$service API calls duration in ms")
                .setUnit("ms")
                .build()
        }
    }

    private fun getApiCallRetryCounter(service: String): LongCounter {
        val s = service.lowercase()
        return apiCallRetries.computeIfAbsent(s) {
            meter.counterBuilder("aws.${s}.api_call.retry")
                .setDescription("Total number of retry when call $s api")
                .build()
        }
    }

    override fun publish(metricCollection: MetricCollection) {
        try {
            val r = Recorder(metricCollection)
            r.record()
        } catch (e: Exception) {
            logger.error("Unexpected exception while recording {}", metricCollection, e)
        }
    }

    override fun close() {}

    inner class Recorder(private val metricCollection: MetricCollection) {
        private val contexts: Deque<ApiCallContext> = ArrayDeque()

        fun record() = record(metricCollection)

        fun record(mc: MetricCollection) {
            var newApiCall = false
            try {
                when (mc.name()) {
                    "ApiCall" -> newApiCall = recordApiCallMetrics(mc)
                    "ApiCallAttempt" -> recordApiCallAttemptMetrics(mc)
                }
                mc.children().forEach(::record)
            } finally {
                if (newApiCall) contexts.pop()
            }
        }

        private fun recordApiCallMetrics(mc: MetricCollection): Boolean {
            val service = mc.metricValues(SERVICE_ID).firstOrNull() ?: return false
            val operation = mc.metricValues(OPERATION_NAME).firstOrNull() ?: return false
            val attrs = Attributes.of(AWS_OPERATION, operation)
            contexts.push(ApiCallContext(service, operation))

            val mDuration = getApiCallDurationHistogram(service)
            mc.metricValues(API_CALL_DURATION).forEach {
                mDuration.record(it.toMillis(), attrs)
            }

            val mRetry = getApiCallRetryCounter(service)
            mc.metricValues(RETRY_COUNT).forEach {
                mRetry.add(it.toLong(), attrs)
            }

            return true
        }

        private fun recordApiCallAttemptMetrics(mc: MetricCollection) {
            val code = mc.metricValues(HTTP_STATUS_CODE).firstOrNull() ?: return
            val ab = Attributes.builder()
                .put(HTTP_RESPONSE_STATUS_CODE, code.toString())
            val ctx = contexts.peekFirst()
            if (ctx != null) {
                ab.put(AWS_SERVICE, ctx.service)
                ab.put(AWS_OPERATION, ctx.operation)
            }
            val attrs = ab.build()

            mc.metricValues(SERVICE_CALL_DURATION).forEach {
                serviceCallDurations.record(it.toMillis(), attrs)
            }
        }
    }

    data class ApiCallContext(
        val service: String,
        val operation: String,
    )
}
