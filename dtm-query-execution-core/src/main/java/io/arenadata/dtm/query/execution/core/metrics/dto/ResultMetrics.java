package io.arenadata.dtm.query.execution.core.metrics.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultMetrics {
    private Boolean isMetricsEnabled;
    private List<RequestStats> statistics;
}