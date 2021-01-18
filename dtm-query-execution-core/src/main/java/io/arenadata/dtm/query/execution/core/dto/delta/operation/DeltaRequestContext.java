package io.arenadata.dtm.query.execution.core.dto.delta.operation;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlDeltaCall;
import io.arenadata.dtm.query.execution.core.dto.CoreRequestContext;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.DELTA;

@Getter
@Setter
@ToString
public class DeltaRequestContext<S extends SqlDeltaCall> extends CoreRequestContext<DatamartRequest, S> {
    private List<PostSqlActionType> postActions;

    public DeltaRequestContext(DatamartRequest request, String envName, S sqlNode) {
        super(request, envName, sqlNode, metrics);
    }

    public DeltaRequestContext(DatamartRequest request, String envName, RequestMetrics metrics, S sqlNode) {
        super(request, envName, metrics, sqlNode);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DELTA;
    }
}
