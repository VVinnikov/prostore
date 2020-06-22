package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;

public interface QueryCostService<T> extends DatamartExecutionService<QueryCostRequestContext, AsyncResult<T>> {

	default SqlProcessingType getSqlProcessingType() {
		return SqlProcessingType.COST;
	}

	void calc(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler);

}
