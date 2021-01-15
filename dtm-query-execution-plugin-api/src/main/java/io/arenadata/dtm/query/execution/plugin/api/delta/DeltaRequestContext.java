package io.arenadata.dtm.query.execution.plugin.api.delta;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

import static io.arenadata.dtm.common.model.SqlProcessingType.DELTA;

@Data
@ToString
public class DeltaRequestContext extends RequestContext<DatamartRequest> {
    private List<PostSqlActionType> postActions;
    public DeltaRequestContext(RequestMetrics metrics, DatamartRequest request) {
        super(request, sqlNode, envName, metrics);
        postActions = new ArrayList<>();
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DELTA;
    }
}
