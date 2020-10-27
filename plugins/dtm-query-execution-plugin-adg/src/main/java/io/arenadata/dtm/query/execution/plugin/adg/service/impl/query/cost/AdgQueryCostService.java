package io.arenadata.dtm.query.execution.plugin.adg.service.impl.query.cost;

import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryCostService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;

@Service("adgQueryCostService")
public class AdgQueryCostService implements QueryCostService<Integer> {

	@Override
	public void calc(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
		handler.handle(Future.succeededFuture(1000));
	}

	@Override
	public void execute(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
		handler.handle(Future.failedFuture(new RuntimeException("Unsupported operation")));
	}
}