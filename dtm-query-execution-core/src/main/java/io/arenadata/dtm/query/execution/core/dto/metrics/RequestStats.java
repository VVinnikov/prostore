package io.arenadata.dtm.query.execution.core.dto.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RequestStats {
    private final SqlProcessingType actionType;
    private final RequestsAllMetrics allStats;
    private final RequestsActiveMetrics activeRequests;
}
