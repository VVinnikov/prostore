package io.arenadata.dtm.query.execution.plugin.api.rollback;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;

public class RollbackRequestContext extends CoreRequestContext<RollbackRequest> {

    public RollbackRequestContext(RequestMetrics metrics, RollbackRequest request) {
        super(request, sqlNode, envName, metrics);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
