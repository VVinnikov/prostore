package io.arenadata.dtm.query.execution.core.delta.dto.operation;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlDeltaCall;
import io.arenadata.dtm.query.execution.core.base.dto.CoreRequestContext;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.DELTA;

@Getter
@Setter
@ToString
public class DeltaRequestContext extends CoreRequestContext<DatamartRequest, SqlDeltaCall> {
    private List<PostSqlActionType> postActions;

    public DeltaRequestContext(RequestMetrics metrics,
                               DatamartRequest request,
                               String envName,
                               SqlDeltaCall sqlNode) {
        super(metrics, envName, request, sqlNode);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DELTA;
    }
}
