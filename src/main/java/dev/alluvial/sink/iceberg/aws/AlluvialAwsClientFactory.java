package dev.alluvial.sink.iceberg.aws;

import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;

import static org.apache.iceberg.aws.AwsClientFactories.*;

// client.factory=dev.alluvial.sink.iceberg.aws.AlluvialAwsClientFactory
public class AlluvialAwsClientFactory implements AwsClientFactory {
    // used to delegate all creator except s3()
    private AwsClientFactory defaultFactory;

    private String s3Endpoint;
    private String s3AccessKeyId;
    private String s3SecretAccessKey;
    private String s3SessionToken;
    private Boolean s3PathStyleAccess;
    private Boolean s3UseArnRegionEnabled;
    private String httpClientType;
    private MetricPublisher metricPublisher;

    @Override
    public S3Client s3() {
        return S3Client.builder()
            .httpClientBuilder(configureHttpClientBuilder(httpClientType))
            .applyMutation(builder -> configureEndpoint(builder, s3Endpoint))
            .serviceConfiguration(s3Configuration(s3PathStyleAccess, s3UseArnRegionEnabled))
            .overrideConfiguration(o -> o.addMetricPublisher(metricPublisher))
            .credentialsProvider(credentialsProvider(s3AccessKeyId, s3SecretAccessKey, s3SessionToken))
            .build();
    }

    @Override
    public GlueClient glue() {
        return defaultFactory.glue();
    }

    @Override
    public KmsClient kms() {
        return defaultFactory.kms();
    }

    @Override
    public DynamoDbClient dynamo() {
        return defaultFactory.dynamo();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.s3Endpoint = properties.get(AwsProperties.S3FILEIO_ENDPOINT);
        this.s3AccessKeyId = properties.get(AwsProperties.S3FILEIO_ACCESS_KEY_ID);
        this.s3SecretAccessKey = properties.get(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY);
        this.s3SessionToken = properties.get(AwsProperties.S3FILEIO_SESSION_TOKEN);
        this.s3PathStyleAccess = PropertyUtil.propertyAsBoolean(
            properties,
            AwsProperties.S3FILEIO_PATH_STYLE_ACCESS,
            AwsProperties.S3FILEIO_PATH_STYLE_ACCESS_DEFAULT
        );
        this.s3UseArnRegionEnabled = PropertyUtil.propertyAsBoolean(
            properties,
            AwsProperties.S3_USE_ARN_REGION_ENABLED,
            AwsProperties.S3_USE_ARN_REGION_ENABLED_DEFAULT
        );
        this.httpClientType = PropertyUtil.propertyAsString(
            properties,
            AwsProperties.HTTP_CLIENT_TYPE,
            AwsProperties.HTTP_CLIENT_TYPE_DEFAULT
        );
        metricPublisher = MicrometerMetricPublisher.getInstance();

        var props = Maps.newHashMap(properties);
        props.remove(AwsProperties.CLIENT_FACTORY);
        defaultFactory = AwsClientFactories.from(props);
    }

    /**
     * Clone from AwsClientFactories
     *
     * @see AwsClientFactories#credentialsProvider(String, String, String)
     */
    static AwsCredentialsProvider credentialsProvider(
        String accessKeyId, String secretAccessKey, String sessionToken) {
        if (accessKeyId != null) {
            if (sessionToken == null) {
                return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey));
            } else {
                return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
            }
        } else {
            return DefaultCredentialsProvider.create();
        }
    }
}
