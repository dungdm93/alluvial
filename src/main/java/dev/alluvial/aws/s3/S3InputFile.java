package dev.alluvial.aws.s3;

import dev.alluvial.aws.AwsProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import software.amazon.awssdk.services.s3.S3Client;

public class S3InputFile extends dev.alluvial.aws.s3.BaseS3File implements InputFile {
    public static S3InputFile fromLocation(String location, S3Client client) {
        return new S3InputFile(client, new S3URI(location), new AwsProperties());
    }

    public static S3InputFile fromLocation(String location, S3Client client, AwsProperties awsProperties) {
        return new S3InputFile(client, new S3URI(location), awsProperties);
    }

    S3InputFile(S3Client client, S3URI uri, AwsProperties awsProperties) {
        super(client, uri, awsProperties);
    }

    /**
     * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
     *
     * @return content length
     */
    @Override
    public long getLength() {
        return getObjectMetadata().contentLength();
    }

    @Override
    public SeekableInputStream newStream() {
        return new S3InputStream(client(), uri(), awsProperties());
    }

}

