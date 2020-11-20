package io.arenadata.dtm.query.execution.plugin.api.rollback;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;

public class RollbackRequestContext extends RequestContext<RollbackRequest> {

    public RollbackRequestContext(RollbackRequest request) {
        super(request);
    }

    public RollbackRequestContext(RequestMetrics metrics, RollbackRequest request) {
        super(metrics, request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
