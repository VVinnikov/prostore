package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.cost;

import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryCostService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;

@Service("adqmQueryCostService")
public class AdqmQueryCostService implements QueryCostService<Integer> {

    @Override
    public void calc(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
        handler.handle(Future.succeededFuture(1));
    }

    @Override
    public void execute(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
        handler.handle(Future.failedFuture(new RuntimeException("Unsupported operation")));
    }
}
