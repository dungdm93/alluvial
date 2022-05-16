package org.apache.iceberg.aws;

import org.apache.iceberg.aws.AwsClientFactories.DefaultAwsClientFactory;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.util.Map;

import static org.apache.iceberg.aws.AwsClientFactories.configureEndpoint;
import static org.apache.iceberg.aws.AwsClientFactories.credentialsProvider;

// client.factory=org.apache.iceberg.aws.AlluvialAwsClientFactory
public class AlluvialAwsClientFactory implements AwsClientFactory {
    private static final SdkHttpClient HTTP_CLIENT_DEFAULT = UrlConnectionHttpClient.create();
    public static final String S3FILEIO_PATH_STYLE_ACCESS = "s3.path-style-access";
    public static final boolean S3FILEIO_PATH_STYLE_ACCESS_DEFAULT = false;

    // used to delegate all creator except s3()
    private final DefaultAwsClientFactory defaultAwsClientFactory = new DefaultAwsClientFactory();

    private String s3Endpoint;
    private String s3AccessKeyId;
    private String s3SecretAccessKey;
    private String s3SessionToken;
    private Boolean s3PathStyleAccess;

    @Override
    public S3Client s3() {
        final S3Configuration config = S3Configuration.builder()
            .applyMutation(builder -> configurePathStyle(builder, s3PathStyleAccess))
            .build();

        return S3Client.builder()
            .httpClient(HTTP_CLIENT_DEFAULT)
            .serviceConfiguration(config)
            .applyMutation(builder -> configureEndpoint(builder, s3Endpoint))
            .credentialsProvider(credentialsProvider(s3AccessKeyId, s3SecretAccessKey, s3SessionToken))
            .build();
    }

    @Override
    public GlueClient glue() {
        return defaultAwsClientFactory.glue();
    }

    @Override
    public KmsClient kms() {
        return defaultAwsClientFactory.kms();
    }

    @Override
    public DynamoDbClient dynamo() {
        return defaultAwsClientFactory.dynamo();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.s3Endpoint = properties.get(AwsProperties.S3FILEIO_ENDPOINT);
        this.s3AccessKeyId = properties.get(AwsProperties.S3FILEIO_ACCESS_KEY_ID);
        this.s3SecretAccessKey = properties.get(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY);
        this.s3SessionToken = properties.get(AwsProperties.S3FILEIO_SESSION_TOKEN);
        this.s3PathStyleAccess = PropertyUtil.propertyAsBoolean(
            properties,
            S3FILEIO_PATH_STYLE_ACCESS,
            S3FILEIO_PATH_STYLE_ACCESS_DEFAULT
        );

        defaultAwsClientFactory.initialize(properties);
    }

    public static void configurePathStyle(S3Configuration.Builder builder, Boolean pathStyleAccess) {
        if (pathStyleAccess) {
            builder.pathStyleAccessEnabled(true);
        }
    }
}
