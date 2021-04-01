package io.arenadata.dtm.query.execution.core.metrics.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
public class RequestsActiveMetrics {
    private Long total;
    private List<ActiveStats> perPlugin;
}
