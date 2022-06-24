/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package dev.alluvial.aws.s3;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;

import dev.alluvial.aws.AwsProperties;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * @link Customizations for S3InputStream retry mechanism.
 * Refer this Pull request - <a href="https://github.com/apache/iceberg/pull/4912">Iceberg PR</a>
 */
class S3InputStream extends SeekableInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

    private final StackTraceElement[] createStack;
    private final S3Client s3;
    private final S3URI location;
    private final AwsProperties awsProperties;

    private InputStream stream;
    private long pos = 0;
    private long next = 0;
    private boolean closed = false;

    private int skipSize = 1024 * 1024;

    S3InputStream(S3Client s3, S3URI location) {
        this(s3, location, new AwsProperties());
    }

    S3InputStream(S3Client s3, S3URI location, AwsProperties awsProperties) {
        this.s3 = s3;
        this.location = location;
        this.awsProperties = awsProperties;

        createStack = Thread.currentThread().getStackTrace();
    }

    @Override
    public long getPos() {
        return next;
    }

    @Override
    public void seek(long newPos) {
        Preconditions.checkState(!closed, "already closed");
        Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);

        // this allows a seek beyond the end of the stream but the next read will fail
        next = newPos;
    }

    @Override
    public int read() throws IOException {
        int[] valueRef = new int[1];
        try {
            createStreamReadTask()
                .run(ignored -> {
                    try {
                        Preconditions.checkState(!closed, "Cannot read: already closed");
                        positionStream();
                        valueRef[0] = stream.read();
                    } catch (IOException e) {
                        closeStream();
                        throw new UncheckedIOException(e);
                    }
                }, IOException.class);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        pos += 1;
        next += 1;

        return valueRef[0];
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int[] bytesReadRef = new int[1];
        boolean thrown = false;
        try {
            createStreamReadTask()
                .run(ignored -> {
                    try {
                        Preconditions.checkState(!closed, "Cannot read: already closed");
                        positionStream();
                        bytesReadRef[0] = stream.read(b, off, len);
                    } catch (IOException e) {
                        closeStream();
                        throw new UncheckedIOException(e);
                    }
                }, IOException.class);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        pos += bytesReadRef[0];
        next += bytesReadRef[0];

        return bytesReadRef[0];
    }

    private static boolean shouldRetry(Exception e) {
        return e instanceof UncheckedIOException && !(e.getCause() instanceof EOFException);
    }

    @Override
    public void close() throws IOException {
        super.close();
        closed = true;
        closeStream();
    }

    private void positionStream() throws IOException {
        if ((stream != null) && (next == pos)) {
            // already at specified position
            return;
        }

        if ((stream != null) && (next > pos)) {
            // seeking forwards
            long skip = next - pos;
            if (skip <= Math.max(stream.available(), skipSize)) {
                // already buffered or seek is small enough
                LOG.debug("Read-through seek for {} to offset {}", location, next);
                try {
                    ByteStreams.skipFully(stream, skip);
                    pos = next;
                    return;
                } catch (IOException ignored) {
                    // will retry by re-opening the stream
                }
            }
        }

        // close the stream and open at desired position
        LOG.debug("Seek with new stream for {} to offset {}", location, next);
        pos = next;
        openStream();
    }

    private void openStream() throws IOException {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
            .bucket(location.bucket())
            .key(location.key())
            .range(String.format("bytes=%s-", pos));

        S3RequestUtil.configureEncryption(awsProperties, requestBuilder);

        closeStream();
        stream = s3.getObject(requestBuilder.build(), ResponseTransformer.toInputStream());
    }

    private void closeStream() throws IOException {
        if (stream != null) {
            try {
                if (stream instanceof Abortable) {
                    // Stated in the ResponseInputStream javadoc:
                    // If it is not desired to read remaining data from the stream,
                    // you can explicitly abort the connection via abort().
                    ((Abortable) stream).abort();
                } else {
                    stream.close();
                }
            } catch (IOException | AbortedException e) {
                // ignore failure to abort or close stream
            } finally {
                stream = null;
            }
        }
    }

    public void setSkipSize(int skipSize) {
        this.skipSize = skipSize;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!closed) {
            close(); // releasing resources is more important than printing the warning
            String trace = Joiner.on("\n\t").join(
                Arrays.copyOfRange(createStack, 1, createStack.length));
            LOG.warn("Unclosed input stream created by:\n\t{}", trace);
        }
    }

    protected Tasks.Builder<Integer> createStreamReadTask() {
        return Tasks.foreach(0)
            .retry(awsProperties.s3ReadRetryNumRetries())
            .exponentialBackoff(
                awsProperties.s3ReadRetryMinWaitMs(),
                awsProperties.s3ReadRetryMaxWaitMs(),
                awsProperties.s3ReadRetryTotalTimeoutMs(),
                2.0
            )
            .shouldRetryTest(S3InputStream::shouldRetry)
            .throwFailureWhenFinished();
    }
}
