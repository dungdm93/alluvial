package dev.alluvial.aws.s3;

import dev.alluvial.aws.AwsProperties;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

abstract class BaseS3File {
    private final S3Client client;
    private final S3URI uri;
    private final AwsProperties awsProperties;
    private HeadObjectResponse metadata;

    BaseS3File(S3Client client, S3URI uri) {
        this(client, uri, new AwsProperties());
    }

    BaseS3File(S3Client client, S3URI uri, AwsProperties awsProperties) {
        this.client = client;
        this.uri = uri;
        this.awsProperties = awsProperties;
    }

    public String location() {
        return uri.location();
    }

    S3Client client() {
        return client;
    }

    S3URI uri() {
        return uri;
    }

    public AwsProperties awsProperties() {
        return awsProperties;
    }

    /**
     * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
     *
     * @return flag
     */
    public boolean exists() {
        try {
            return getObjectMetadata() != null;
        } catch (S3Exception e) {
            if (e.statusCode() == HttpStatusCode.NOT_FOUND) {
                return false;
            } else {
                throw e; // return null if 404 Not Found, otherwise rethrow
            }
        }
    }

    protected HeadObjectResponse getObjectMetadata() throws S3Exception {
        if (metadata == null) {
            HeadObjectRequest.Builder requestBuilder = HeadObjectRequest.builder()
                .bucket(uri().bucket())
                .key(uri().key());
            S3RequestUtil.configureEncryption(awsProperties, requestBuilder);
            metadata = client().headObject(requestBuilder.build());
        }

        return metadata;
    }

    @Override
    public String toString() {
        return uri.toString();
    }

}

