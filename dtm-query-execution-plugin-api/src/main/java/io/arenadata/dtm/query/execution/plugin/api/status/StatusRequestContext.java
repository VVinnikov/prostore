package io.arenadata.dtm.query.execution.plugin.api.status;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.StatusRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;

public class StatusRequestContext extends RequestContext<StatusRequest> {

    public StatusRequestContext(RequestMetrics metrics, StatusRequest request) {
        super(metrics, request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.STATUS;
    }
}
