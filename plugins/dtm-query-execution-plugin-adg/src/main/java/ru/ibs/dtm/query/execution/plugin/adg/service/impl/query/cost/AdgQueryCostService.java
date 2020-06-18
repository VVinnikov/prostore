package ru.ibs.dtm.query.execution.plugin.adg.service.impl.query.cost;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostAnalyzer;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

@Service("adgQueryCostService")
public class AdgQueryCostService implements QueryCostService<Integer> {

	@Override
	public void calc(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
		handler.handle(Future.succeededFuture(1000));
	}

	@Override
	public <S extends QueryCostService<Integer>> void addAnalyzer(QueryCostAnalyzer<Integer, S> analyzer) {
	}

	@Override
	public void execute(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
		handler.handle(Future.failedFuture(new RuntimeException("Unsupported operation")));
	}
}
