package io.arenadata.dtm.query.execution.core.rollback.factory.impl;

import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.factory.RollbackRequestContextFactory;
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
