package io.arenadata.dtm.query.execution.plugin.api.rollback;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;

public class RollbackRequestContext extends RequestContext<RollbackRequest> {

    public RollbackRequestContext(RequestMetrics metrics, RollbackRequest request) {
        super(request, sqlNode, envName, sourceType, metrics);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
