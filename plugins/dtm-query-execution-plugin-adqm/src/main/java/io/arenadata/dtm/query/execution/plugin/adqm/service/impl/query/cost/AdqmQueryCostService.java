package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.cost;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryCostService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;

@Service("adqmQueryCostService")
public class AdqmQueryCostService implements QueryCostService<Integer> {

    @Override
    public void calc(QueryCostRequestContext context, AsyncHandler<Integer> handler) {
        handler.handleSuccess(1);
    }

    @Override
    public void execute(QueryCostRequestContext context, AsyncHandler<Integer> handler) {
        handler.handleError(new DataSourceException("Unsupported operation"));
    }
}
