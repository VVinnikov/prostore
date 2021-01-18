package io.arenadata.dtm.query.execution.plugin.api.status;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.StatusRequest;

public class StatusRequestContext extends CoreRequestContext<StatusRequest> {

    public StatusRequestContext(RequestMetrics metrics, StatusRequest request) {
        super(request, sqlNode, envName, metrics);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.STATUS;
    }
}
