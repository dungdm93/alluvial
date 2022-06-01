package dev.alluvial.aws.metric;

import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.SdkMetric;

import java.util.List;

public class MetricUtils {
    public static <T> List<T> extractAll(MetricCollection collection, SdkMetric<T> awsMetric) {
        return collection.metricValues(awsMetric);
    }

    public static <T> T extractFirst(MetricCollection collection, SdkMetric<T> awsMetric) {
        List<T> values = extractAll(collection, awsMetric);
        if (values.size() > 0) return values.get(0);
        return null;
    }
}
