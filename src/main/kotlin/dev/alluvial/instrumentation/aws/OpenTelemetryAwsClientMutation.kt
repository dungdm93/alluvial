package dev.alluvial.instrumentation.aws

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry
import org.apache.iceberg.aws.AwsClientMutation
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.metrics.MetricPublisher

/**
 * @see autoconfigure[https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/instrumentation/aws-sdk/aws-sdk-2.2/library/README.md]
 */
class OpenTelemetryAwsClientMutation : AwsClientMutation {
    private lateinit var interceptor: ExecutionInterceptor
    private lateinit var metricPublisher: MetricPublisher

    override fun initialize(properties: MutableMap<String, String>) {
        val telemetry = AwsSdkTelemetry.create(GlobalOpenTelemetry.get())
        interceptor = telemetry.newExecutionInterceptor()
        metricPublisher = OpenTelemetryMetricPublisher(GlobalOpenTelemetry.get())
    }

    override fun <B : AwsClientBuilder<B, *>> mutate(builder: B) {
        builder.overrideConfiguration {
            it.addExecutionInterceptor(interceptor)
            it.addMetricPublisher(metricPublisher)
        }
    }
}
