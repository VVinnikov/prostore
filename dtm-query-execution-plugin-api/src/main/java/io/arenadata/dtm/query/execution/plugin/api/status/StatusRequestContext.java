package io.arenadata.dtm.query.execution.plugin.api.status;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.StatusRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;

public class StatusRequestContext extends RequestContext<StatusRequest> {

    public StatusRequestContext(StatusRequest request) {
        super(request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.STATUS;
    }
}