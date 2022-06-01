package dev.alluvial.aws.metric;

import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to publish AWS metric collection to multiple metric collectors.
 * Metric collections are passed to collectors by service ID.
 * @link <a href="https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/metrics-list.html">Service client metrics</a>
 */
public class MicrometerMetricPublisher implements MetricPublisher {
    private static MicrometerMetricPublisher instance;
    private static final List<ClientMetricCollector> collectors = new ArrayList<>();

    private MicrometerMetricPublisher() {}

    public static void registerCollector(ClientMetricCollector collector) {
        MicrometerMetricPublisher.collectors.add(collector);
    }

    @Override
    public void publish(MetricCollection metricCollection) {
        collectors.forEach(c -> c.collect(metricCollection));

        metricCollection.children().forEach(this::publish);
    }

    @Override
    public void close() {
        collectors.forEach(ClientMetricCollector::close);
    }

    synchronized public static MicrometerMetricPublisher getInstance() {
        if (instance == null) {
            instance = new MicrometerMetricPublisher();
        }
        return instance;
    }
}
