package org.apache.iceberg.aws.s3;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.BaseAwsClientFactory;
import org.apache.iceberg.aws.HttpClientProperties;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;

public class AlluvialS3FileIOAwsClientFactory
        extends BaseAwsClientFactory
        implements S3FileIOAwsClientFactory {
    private S3FileIOProperties s3FileIOProperties;
    private HttpClientProperties httpClientProperties;
    private AwsClientProperties awsClientProperties;

    AlluvialS3FileIOAwsClientFactory() {
        this.s3FileIOProperties = new S3FileIOProperties();
        this.httpClientProperties = new HttpClientProperties();
        this.awsClientProperties = new AwsClientProperties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.awsClientProperties = new AwsClientProperties(properties);
        this.httpClientProperties = new HttpClientProperties(properties);
        super.initialize("s3", properties);
    }

    @Override
    public S3Client s3() {
        var builder = S3Client.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .applyMutation(s3FileIOProperties::applyServiceConfigurations)
                .applyMutation(
                        s3ClientBuilder ->
                                s3FileIOProperties.applyCredentialConfigurations(
                                        awsClientProperties, s3ClientBuilder))
                .applyMutation(s3FileIOProperties::applySignerConfiguration);

        applyMutations(builder);

        return builder.build();
    }
}
