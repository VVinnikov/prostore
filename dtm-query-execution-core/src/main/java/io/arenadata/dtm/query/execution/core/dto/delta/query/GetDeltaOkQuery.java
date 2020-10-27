package io.arenadata.dtm.query.execution.core.dto.delta.query;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
import java.util.UUID;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.GET_DELTA_OK;

@Data
@EqualsAndHashCode(callSuper = true)
public class GetDeltaOkQuery extends DeltaQuery {
    private Long cnFrom;
    private Long cnTo;

    @Builder
    public GetDeltaOkQuery(UUID requestId, String datamart, Long deltaNum, LocalDateTime deltaDate, Long cnFrom, Long cnTo) {
        super(requestId, datamart, deltaNum, deltaDate);
        this.cnFrom = cnFrom;
        this.cnTo = cnTo;
    }

    @Override
    public DeltaAction getDeltaAction() {
        return GET_DELTA_OK;
    }
}
