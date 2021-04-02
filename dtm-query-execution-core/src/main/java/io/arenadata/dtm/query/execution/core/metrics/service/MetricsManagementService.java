package io.arenadata.dtm.query.execution.core.metrics.service;

import io.arenadata.dtm.query.execution.core.metrics.dto.MetricsSettingsUpdateResult;

public interface MetricsManagementService {

    MetricsSettingsUpdateResult turnOnMetrics();

    MetricsSettingsUpdateResult turnOffMetrics();
}
