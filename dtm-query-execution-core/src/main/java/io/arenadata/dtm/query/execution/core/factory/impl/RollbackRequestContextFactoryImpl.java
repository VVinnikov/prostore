package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.query.execution.core.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.request.RollbackRequest;
import io.arenadata.dtm.query.execution.core.dto.rollback.RollbackRequestContext;
import org.springframework.stereotype.Component;

@Component
public class RollbackRequestContextFactoryImpl implements RollbackRequestContextFactory {

    @Override
    public RollbackRequestContext create(EdmlRequestContext context) {
        return new RollbackRequestContext(
                context.getMetrics(),
                context.getEnvName(),
                RollbackRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .datamart(context.getSourceEntity().getSchema())
                .destinationTable(context.getDestinationEntity().getName())
                .sysCn(context.getSysCn())
                .entity(context.getDestinationEntity())
                .build(),
                context.getSqlNode());
    }
}
