package ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

@Service("adbQueryCostService")
public class AdbQueryCostService implements QueryCostService<Integer> {

	@Override
	public void calc(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
		handler.handle(Future.succeededFuture(0));
	}

	@Override
	public void execute(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
		handler.handle(Future.failedFuture(new RuntimeException("Unsupported operation")));
	}
}
