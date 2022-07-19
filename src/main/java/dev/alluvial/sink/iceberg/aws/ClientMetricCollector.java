package dev.alluvial.sink.iceberg.aws;

import software.amazon.awssdk.metrics.MetricCollection;

import java.io.Closeable;

public interface ClientMetricCollector extends Closeable {
    void collect(MetricCollection metricCollection);

    void close();
}
