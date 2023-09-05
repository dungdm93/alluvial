package org.apache.iceberg.aws;

import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;

import java.util.Map;

public interface AwsClientMutation {
    void initialize(Map<String, String> properties);

    <B extends AwsClientBuilder<B, ?>> void mutate(B builder);
}
