package io.arenadata.dtm.query.execution.core.dto.metrics;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class RequestsAllMetrics {
    private final Long total;
    private final List<AllStats> perPlugin;
}
