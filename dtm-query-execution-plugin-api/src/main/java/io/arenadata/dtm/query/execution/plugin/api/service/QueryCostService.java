package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface QueryCostService<T> extends DatamartExecutionService<QueryCostRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.COST;
    }

    void calc(QueryCostRequestContext context, AsyncHandler<Integer> handler);

}
