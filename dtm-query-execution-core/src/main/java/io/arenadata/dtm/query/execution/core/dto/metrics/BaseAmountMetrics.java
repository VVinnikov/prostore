package io.arenadata.dtm.query.execution.core.dto.metrics;

import io.arenadata.dtm.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class BaseAmountMetrics {
    private final Long amountTotal;
    private final Map<SourceType, Long> amountPerPlugin;
}
