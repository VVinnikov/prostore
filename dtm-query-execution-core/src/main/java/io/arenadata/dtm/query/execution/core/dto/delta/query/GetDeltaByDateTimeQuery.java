package io.arenadata.dtm.query.execution.core.dto.delta.query;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
import java.util.UUID;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.GET_DELTA_BY_DATETIME;

@Data
@EqualsAndHashCode(callSuper = true)
public class GetDeltaByDateTimeQuery extends DeltaQuery {

    @Builder
    public GetDeltaByDateTimeQuery(UUID requestId, String datamart, Long deltaNum, LocalDateTime deltaDate) {
        super(requestId, datamart, deltaNum, deltaDate);
    }

    @Override
    public DeltaAction getDeltaAction() {
        return GET_DELTA_BY_DATETIME;
    }
}
