package io.arenadata.dtm.query.execution.core.dto.delta.query;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.UUID;

@AllArgsConstructor
@Data
public abstract class DeltaQuery {
    private UUID requestId;
    private String datamart;
    private Long deltaNum;
    private LocalDateTime deltaDate;

    public abstract DeltaAction getDeltaAction();
}
