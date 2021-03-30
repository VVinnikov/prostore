package io.arenadata.dtm.query.execution.core.dto.metrics;

import io.arenadata.dtm.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AllStats {
    private SourceType sourceType;
    private CountMetrics countMetrics;
    private TimeMetrics timeMetrics;
}
