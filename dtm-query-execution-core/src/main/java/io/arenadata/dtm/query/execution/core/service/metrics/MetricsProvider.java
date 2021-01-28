package io.arenadata.dtm.query.execution.core.service.metrics;

import io.arenadata.dtm.query.execution.core.dto.metrics.ResultMetrics;

public interface MetricsProvider {

    ResultMetrics get();

    void clear();
}
