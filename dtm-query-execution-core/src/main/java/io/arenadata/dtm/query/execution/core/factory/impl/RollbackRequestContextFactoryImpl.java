package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.query.execution.core.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import org.springframework.stereotype.Component;

@Component
public class RollbackRequestContextFactoryImpl implements RollbackRequestContextFactory {

    @Override
    public RollbackRequestContext create(EdmlRequestContext context) {
        return new RollbackRequestContext(RollbackRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .datamart(context.getSourceTable().getSchemaName())
                .destinationTable(context.getDestinationTable().getTableName())
                .sysCn(context.getSysCn())
                .entity(context.getDestinationEntity())
                .build());
    }
}
