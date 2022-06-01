package dev.alluvial.aws.metric;

import software.amazon.awssdk.metrics.MetricCollection;

import java.io.Closeable;

public interface ClientMetricCollector extends Closeable {
    void collect(MetricCollection metricCollection);
    void close();
}
