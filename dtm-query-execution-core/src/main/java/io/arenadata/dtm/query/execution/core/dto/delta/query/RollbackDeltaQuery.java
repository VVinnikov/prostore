package io.arenadata.dtm.query.execution.core.dto.delta.query;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlRollbackDelta;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = false)
public class RollbackDeltaQuery extends DeltaQuery {

    private String envName;
    private RequestMetrics requestMetrics;
    private SqlRollbackDelta sqlNode;

    @Builder
    public RollbackDeltaQuery(QueryRequest request,
                              String datamart,
                              Long deltaNum,
                              LocalDateTime deltaDate,
                              String envName,
                              SqlRollbackDelta sqlNode) {
        super(request, datamart, deltaNum, deltaDate);
        this.envName = envName;
        this.sqlNode = sqlNode;
    }

    @Override
    public DeltaAction getDeltaAction() {
        return DeltaAction.ROLLBACK_DELTA;
    }
}
