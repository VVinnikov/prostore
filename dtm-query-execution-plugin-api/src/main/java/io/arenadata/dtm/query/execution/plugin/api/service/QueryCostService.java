package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.vertx.core.Future;

public interface QueryCostService<T> extends DatamartExecutionService<QueryCostRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.COST;
    }

    Future<Integer> calc(QueryCostRequestContext context);

}
