package dev.alluvial.aws.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.metrics.MetricCollection;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static software.amazon.awssdk.core.metrics.CoreMetric.*;

public class ServiceClientMetricCollector implements ClientMetricCollector {
    private static final Logger logger = LoggerFactory.getLogger(ClientMetricCollector.class);
    private static final String AWS_METRIC_NAME = "ApiCall";
    private final MeterRegistry registry;
    private final String serviceId;
    private final Tags tags;
    private final String prefix;
    private final Map<String, OperationMetrics> operationMetrics;

    public ServiceClientMetricCollector(String serviceId, MeterRegistry registry, Tags tags) {
        this.serviceId = serviceId;
        this.registry = registry;
        this.tags = tags.and("service", serviceId);
        this.prefix = String.format("aws.%s", serviceId.toLowerCase());
        this.operationMetrics = new ConcurrentHashMap<>();
    }

    public void collect(MetricCollection metricCollection) {
        if (!isQualified(metricCollection)) return;

        var operationName = MetricUtils.extractFirst(metricCollection, OPERATION_NAME);
        var metrics = operationMetrics.computeIfAbsent(
            operationName,
            name -> {
                var om = new OperationMetrics(name);
                om.bindTo(registry);
                return om;
            }
        );

        var requestSuccess = MetricUtils.extractFirst(metricCollection, API_CALL_SUCCESSFUL);
        metrics.incrementStatus(requestSuccess);

        var retryCounts = MetricUtils.extractAll(metricCollection, RETRY_COUNT);
        retryCounts.forEach(metrics::incrementRetry);

        var requestDurations = MetricUtils.extractAll(metricCollection, API_CALL_DURATION);
        requestDurations.forEach(metrics::recordDuration);
    }

    public void close() {
        logger.info("Closing ServiceClientMetricCollector({})", serviceId);
        operationMetrics.values()
            .forEach(metrics -> metrics.unbindFrom(registry));
        operationMetrics.clear();
    }

    private boolean isQualified(MetricCollection metricCollection) {
        if (!metricCollection.name().equals(AWS_METRIC_NAME)) return false;

        var mServiceId = MetricUtils.extractFirst(metricCollection, SERVICE_ID);
        return Objects.equals(mServiceId, serviceId);
    }

    private class OperationMetrics implements MeterBinder {
        private final Tags mTags;
        private Counter successCounter;
        private Counter failureCounter;
        private Counter retryCounter;
        private Timer durationTimer;

        public OperationMetrics(String operationName) {
            this.mTags = tags.and("op", operationName);
        }

        public List<Meter> getMeters() {
            return List.of(successCounter, failureCounter, retryCounter, durationTimer);
        }

        public void incrementStatus(Boolean status) {
            if (TRUE.equals(status)) {
                successCounter.increment();
            } else if (FALSE.equals(status)) {
                failureCounter.increment();
            }
        }

        public void incrementRetry(int value) {
            retryCounter.increment(value);
        }

        public void recordDuration(Duration value) {
            durationTimer.record(value);
        }

        synchronized public void bindTo(@NotNull MeterRegistry registry) {
            this.successCounter = Counter.builder(String.format("%s.api.call", prefix))
                .description("Success API call count")
                .tags(mTags.and("status", "success"))
                .register(registry);

            this.failureCounter = Counter.builder(String.format("%s.api.call", prefix))
                .description("Failed API call count")
                .tags(mTags.and("status", "failure"))
                .register(registry);

            this.retryCounter = Counter.builder(String.format("%s.api.call.retry", prefix))
                .description("API call retry count")
                .tags(mTags)
                .register(registry);

            this.durationTimer = Timer.builder(String.format("%s.api.call.duration", prefix))
                .description("AWS API call duration")
                .publishPercentileHistogram()
                .maximumExpectedValue(Duration.ofMinutes(5))
                .tags(mTags)
                .register(registry);
        }

        synchronized public void unbindFrom(MeterRegistry registry) {
            getMeters().forEach(registry::remove);
        }
    }
}
