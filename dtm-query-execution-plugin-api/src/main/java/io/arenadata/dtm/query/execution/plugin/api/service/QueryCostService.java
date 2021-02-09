package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.vertx.core.Future;

public interface QueryCostService<T> extends DatamartExecutionService<QueryCostRequest, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.COST;
    }

    Future<Integer> calc(QueryCostRequest context);

}
