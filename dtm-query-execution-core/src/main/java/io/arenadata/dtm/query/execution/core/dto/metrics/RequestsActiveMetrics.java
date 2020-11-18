package io.arenadata.dtm.query.execution.core.dto.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RequestsActiveMetrics {
    private final Long total;
    private final List<ActiveStats> perPlugin;
}
