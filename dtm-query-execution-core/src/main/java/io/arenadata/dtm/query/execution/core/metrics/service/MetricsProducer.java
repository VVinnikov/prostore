package io.arenadata.dtm.query.execution.core.metrics.service;

import io.arenadata.dtm.common.metrics.MetricsTopic;

public interface MetricsProducer {

    void publish(MetricsTopic metricsTopic, Object value);
}
