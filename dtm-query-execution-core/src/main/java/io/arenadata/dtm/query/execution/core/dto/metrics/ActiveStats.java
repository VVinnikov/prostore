package io.arenadata.dtm.query.execution.core.dto.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.arenadata.dtm.common.reader.SourceType;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActiveStats {
    private final SourceType sourceType;
    private final TimeMetrics times;
}
