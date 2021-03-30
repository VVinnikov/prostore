package io.arenadata.dtm.query.execution.core.service.metrics;

import io.arenadata.dtm.common.metrics.RequestMetrics;

public interface MetricsProcessingService<T extends RequestMetrics> {

    void process(T metricsValue);
}
