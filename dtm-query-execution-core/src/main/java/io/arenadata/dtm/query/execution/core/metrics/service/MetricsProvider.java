package io.arenadata.dtm.query.execution.core.metrics.service;

import io.arenadata.dtm.query.execution.core.metrics.dto.ResultMetrics;

public interface MetricsProvider {

    ResultMetrics get();

    void clear();
}
