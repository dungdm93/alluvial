package dev.alluvial.instrumentation.aws

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry
import org.apache.iceberg.aws.AwsClientMutation
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor

/**
 * @see autoconfigure[https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/instrumentation/aws-sdk/aws-sdk-2.2/library/README.md]
 */
class OpenTelemetryAwsClientMutation : AwsClientMutation {
    private lateinit var interceptor: ExecutionInterceptor
    override fun initialize(properties: MutableMap<String, String>) {
        val telemetry = AwsSdkTelemetry.create(GlobalOpenTelemetry.get())
        interceptor = telemetry.newExecutionInterceptor()
    }

    override fun <B : AwsClientBuilder<B, *>> mutate(builder: B) {
        builder.overrideConfiguration {
            it.addExecutionInterceptor(interceptor)
        }
    }
}
