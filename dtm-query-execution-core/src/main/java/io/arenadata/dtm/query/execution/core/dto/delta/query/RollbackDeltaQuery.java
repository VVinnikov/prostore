package io.arenadata.dtm.query.execution.core.dto.delta.query;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = false)
public class RollbackDeltaQuery extends DeltaQuery {

    private RequestMetrics requestMetrics;

    @Builder
    public RollbackDeltaQuery(QueryRequest request,
                              String datamart,
                              Long deltaNum,
                              LocalDateTime deltaDate) {
        super(request, datamart, deltaNum, deltaDate);
    }

    @Override
    public DeltaAction getDeltaAction() {
        return DeltaAction.ROLLBACK_DELTA;
    }
}
