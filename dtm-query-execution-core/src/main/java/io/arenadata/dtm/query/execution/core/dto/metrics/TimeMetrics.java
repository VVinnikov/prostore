package io.arenadata.dtm.query.execution.core.dto.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TimeMetrics {
    private Long count;
    private Long totalTimeMs;
    private Long meanTimeMs;
    private Long maxTimeMs;
}
