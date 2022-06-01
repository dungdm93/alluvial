package dev.alluvial.aws.metric;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static software.amazon.awssdk.core.metrics.CoreMetric.SERVICE_CALL_DURATION;
import static software.amazon.awssdk.http.HttpMetric.HTTP_STATUS_CODE;

import software.amazon.awssdk.metrics.MetricCollection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HttpClientMetricCollector implements ClientMetricCollector {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientMetricCollector.class);
    private static final String AWS_METRIC_NAME = "ApiCallAttempt";
    private static final String prefix = "aws.http";
    private final MeterRegistry registry;
    private final Tags tags;
    private final Map<Integer, Timer> httpCodeTimers;

    public HttpClientMetricCollector(MeterRegistry registry, Tags tags) {
        this.registry = registry;
        this.tags = tags;
        this.httpCodeTimers = new ConcurrentHashMap<>();
    }

    public void collect(MetricCollection metricCollection) {
        if (!metricCollection.name().equals(AWS_METRIC_NAME)) return;

        var httpCode = MetricUtils.extractFirst(metricCollection, HTTP_STATUS_CODE);
        if (httpCode == null) return;

        var timer = httpCodeTimers.computeIfAbsent(
            httpCode,
            code -> Timer.builder(String.format("%s.code", prefix))
                .description("AWS client HTTP code")
                .tags(tags.and("code", code.toString()))
                .register(registry)
        );
        metricCollection.metricValues(SERVICE_CALL_DURATION).forEach(timer::record);
    }

    public void close() {
        logger.info("Close AWS client HTTP metric collector");
        httpCodeTimers.values().forEach(Timer::close);
        httpCodeTimers.clear();
    }
}
