package io.arenadata.dtm.query.execution.core.dto.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RequestStats {
    private SqlProcessingType actionType;
    private RequestsAllMetrics allStats;
    private RequestsActiveMetrics activeRequests;
}
