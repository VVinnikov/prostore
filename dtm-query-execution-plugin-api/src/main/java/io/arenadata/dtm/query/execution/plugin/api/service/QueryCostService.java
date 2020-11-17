package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface QueryCostService<T> extends DatamartExecutionService<QueryCostRequestContext, AsyncResult<T>> {

	default SqlProcessingType getSqlProcessingType() {
		return SqlProcessingType.COST;
	}

	void calc(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler);

}
