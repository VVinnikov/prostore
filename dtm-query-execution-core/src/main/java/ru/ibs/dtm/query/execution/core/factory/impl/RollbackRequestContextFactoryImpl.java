package ru.ibs.dtm.query.execution.core.factory.impl;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.factory.RollbackRequestContextFactory;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.RollbackRequest;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;

@Component
public class RollbackRequestContextFactoryImpl implements RollbackRequestContextFactory {

    @Override
    public RollbackRequestContext create(EdmlRequestContext context) {
        return new RollbackRequestContext(RollbackRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .datamart(context.getSourceTable().getSchemaName())
                .targetTable(context.getTargetTable().getTableName())
                .sysCn(context.getSysCn())
                .entity(context.getEntity())
                .build());
    }
}
