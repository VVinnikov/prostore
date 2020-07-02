package ru.ibs.dtm.query.execution.plugin.api.status;

import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.StatusRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

public class StatusRequestContext extends RequestContext<StatusRequest> {

    public StatusRequestContext(StatusRequest request) {
        super(request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.STATUS;
    }
}
