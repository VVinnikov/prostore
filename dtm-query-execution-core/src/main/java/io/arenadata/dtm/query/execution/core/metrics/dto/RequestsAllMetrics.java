package io.arenadata.dtm.query.execution.core.metrics.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RequestsAllMetrics {
    private Long total;
    private List<AllStats> perPlugin;
}
