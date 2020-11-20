package io.arenadata.dtm.query.execution.core.service.metrics;

import io.arenadata.dtm.query.execution.core.dto.metrics.MetricsSettingsUpdateResult;

public interface MetricsManagementService {

    MetricsSettingsUpdateResult turnOnMetrics();

    MetricsSettingsUpdateResult turnOffMetrics();
}
