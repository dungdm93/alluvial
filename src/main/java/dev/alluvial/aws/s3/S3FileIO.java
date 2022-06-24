package dev.alluvial.aws.s3;

import dev.alluvial.aws.AwsProperties;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @link Customizations for S3InputStream retry mechanism.
 * Refer this Pull request - <a href="https://github.com/apache/iceberg/pull/4912">Iceberg PR</a>
 */
public class S3FileIO implements FileIO {
    private SerializableSupplier<S3Client> s3;
    private AwsProperties awsProperties;
    private AwsClientFactory awsClientFactory;
    private transient S3Client client;
    private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);

    /**
     * No-arg constructor to load the FileIO dynamically.
     * <p>
     * All fields are initialized by calling {@link S3FileIO#initialize(Map)} later.
     */
    public S3FileIO() {
    }

    /**
     * Constructor with custom s3 supplier and default AWS properties.
     * <p>
     * Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
     * @param s3 s3 supplier
     */
    public S3FileIO(SerializableSupplier<S3Client> s3) {
        this(s3, new AwsProperties());
    }

    /**
     * Constructor with custom s3 supplier and AWS properties.
     * <p>
     * Calling {@link S3FileIO#initialize(Map)} will overwrite information set in this constructor.
     * @param s3 s3 supplier
     * @param awsProperties aws properties
     */
    public S3FileIO(SerializableSupplier<S3Client> s3, AwsProperties awsProperties) {
        this.s3 = s3;
        this.awsProperties = awsProperties;
    }

    @Override
    public InputFile newInputFile(String path) {
        return S3InputFile.fromLocation(path, client(), awsProperties);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return S3OutputFile.fromLocation(path, client(), awsProperties);
    }

    @Override
    public void deleteFile(String path) {
        S3URI location = new S3URI(path);
        DeleteObjectRequest deleteRequest =
            DeleteObjectRequest.builder().bucket(location.bucket()).key(location.key()).build();

        client().deleteObject(deleteRequest);
    }

    private S3Client client() {
        if (client == null) {
            client = s3.get();
        }
        return client;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.awsProperties = new AwsProperties(properties);
        this.awsClientFactory = AwsClientFactories.from(properties);
        this.s3 = awsClientFactory::s3;
    }

    @Override
    public void close() {
        // handles concurrent calls to close()
        if (isResourceClosed.compareAndSet(false, true)) {
            if (client != null) {
                client.close();
            }
        }
    }
}

