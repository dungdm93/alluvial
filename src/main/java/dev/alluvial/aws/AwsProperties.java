package dev.alluvial.aws;

import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

/**
 * @link Customizations for S3InputStream retry mechanism.
 * Refer this Pull request - <a href="https://github.com/apache/iceberg/pull/4912">Iceberg PR</a>
 */
public class AwsProperties extends org.apache.iceberg.aws.AwsProperties {
    /**
     * Number of times to retry S3 read operation.
     */
    public static final String S3_READ_RETRY_NUM_RETRIES = "s3.read.retry.num-retries";
    public static final int S3_READ_RETRY_NUM_RETRIES_DEFAULT = 3;

    /**
     * Minimum wait time to retry a S3 read operation
     */
    public static final String S3_READ_RETRY_MIN_WAIT_MS = "s3.read.retry.min-wait-ms";
    public static final long S3_READ_RETRY_MIN_WAIT_MS_DEFAULT = 500; // 0.5 seconds

    /**
     * Maximum wait time to retry a S3 read operation
     */
    public static final String S3_READ_RETRY_MAX_WAIT_MS = "s3.read.retry.max-wait-ms";
    public static final long S3_READ_RETRY_MAX_WAIT_MS_DEFAULT = 2 * 60 * 1000; // 2 minute

    /**
     * Total retry time for a S3 read operation
     */
    public static final String S3_READ_RETRY_TOTAL_TIMEOUT_MS = "s3.read.retry.total-timeout-ms";
    public static final long S3_READ_RETRY_TOTAL_TIMEOUT_MS_DEFAULT = 10 * 60 * 1000; // 10 minutes

    private int s3ReadRetryNumRetries;
    private long s3ReadRetryMinWaitMs;
    private long s3ReadRetryMaxWaitMs;
    private long s3ReadRetryTotalTimeoutMs;

    public AwsProperties() {
        super();

        this.s3ReadRetryNumRetries = S3_READ_RETRY_NUM_RETRIES_DEFAULT;
        this.s3ReadRetryMinWaitMs = S3_READ_RETRY_MIN_WAIT_MS_DEFAULT;
        this.s3ReadRetryMaxWaitMs = S3_READ_RETRY_MAX_WAIT_MS_DEFAULT;
        this.s3ReadRetryTotalTimeoutMs = S3_READ_RETRY_TOTAL_TIMEOUT_MS_DEFAULT;
    }

    public AwsProperties(Map<String, String> properties) {
        super(properties);

        this.s3ReadRetryNumRetries = PropertyUtil.propertyAsInt(properties, S3_READ_RETRY_NUM_RETRIES,
            S3_READ_RETRY_NUM_RETRIES_DEFAULT);
        this.s3ReadRetryMinWaitMs = PropertyUtil.propertyAsLong(properties, S3_READ_RETRY_MIN_WAIT_MS,
            S3_READ_RETRY_MIN_WAIT_MS_DEFAULT);
        this.s3ReadRetryMaxWaitMs = PropertyUtil.propertyAsLong(properties, S3_READ_RETRY_MAX_WAIT_MS,
            S3_READ_RETRY_MAX_WAIT_MS_DEFAULT);
        this.s3ReadRetryTotalTimeoutMs = PropertyUtil.propertyAsLong(properties, S3_READ_RETRY_TOTAL_TIMEOUT_MS,
            S3_READ_RETRY_TOTAL_TIMEOUT_MS_DEFAULT);
    }

    public int s3ReadRetryNumRetries() {
        return s3ReadRetryNumRetries;
    }

    public void setS3ReadRetryNumRetries(int s3ReadRetryNumRetries) {
        this.s3ReadRetryNumRetries = s3ReadRetryNumRetries;
    }

    public long s3ReadRetryMinWaitMs() {
        return s3ReadRetryMinWaitMs;
    }

    public void setS3ReadRetryMinWaitMs(long s3ReadRetryMinWaitMs) {
        this.s3ReadRetryMinWaitMs = s3ReadRetryMinWaitMs;
    }

    public long s3ReadRetryMaxWaitMs() {
        return s3ReadRetryMaxWaitMs;
    }

    public void setS3ReadRetryMaxWaitMs(long s3ReadRetryMaxWaitMs) {
        this.s3ReadRetryMaxWaitMs = s3ReadRetryMaxWaitMs;
    }

    public long s3ReadRetryTotalTimeoutMs() {
        return s3ReadRetryTotalTimeoutMs;
    }

    public void setS3ReadRetryTotalTimeoutMs(long s3ReadRetryTotalTimeoutMs) {
        this.s3ReadRetryTotalTimeoutMs = s3ReadRetryTotalTimeoutMs;
    }
}
