package io.arenadata.dtm.query.execution.core.dto.delta.query;

import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@AllArgsConstructor
@Data
public abstract class DeltaQuery {
    private QueryRequest request;
    private String datamart;
    private Long deltaNum;
    private LocalDateTime deltaDate;

    public abstract DeltaAction getDeltaAction();
}
