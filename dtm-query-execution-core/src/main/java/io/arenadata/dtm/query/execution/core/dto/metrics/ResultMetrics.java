package io.arenadata.dtm.query.execution.core.dto.metrics;

import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class ResultMetrics {
    private final Map<SqlProcessingType, BaseAmountMetrics> amounts;
}
