package io.arenadata.dtm.query.execution.core.dto.metrics;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MetricsSettingsUpdateResult {
    private final boolean isMetricsEnabled;
    private final String message;
}
