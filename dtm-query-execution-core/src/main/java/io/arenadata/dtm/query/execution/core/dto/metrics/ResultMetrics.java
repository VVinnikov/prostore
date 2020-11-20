package io.arenadata.dtm.query.execution.core.dto.metrics;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class ResultMetrics {
    private final Boolean isMetricsEnabled;
    private final List<RequestStats> statistics;
}
