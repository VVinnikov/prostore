package io.arenadata.dtm.query.execution.core.dto.metrics;

import io.arenadata.dtm.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AllStats {
    private final SourceType sourceType;
    private final CountMetrics countMetrics;
    private final TimeMetrics timeMetrics;
}
