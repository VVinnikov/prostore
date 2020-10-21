package ru.ibs.dtm.query.execution.plugin.api.rollback;

import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.RollbackRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

public class RollbackRequestContext extends RequestContext<RollbackRequest> {

    public RollbackRequestContext(RollbackRequest request) {
        super(request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
