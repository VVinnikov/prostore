package io.arenadata.dtm.query.execution.plugin.adg.service.impl.query.cost;

import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryCostService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adgQueryCostService")
public class AdgQueryCostService implements QueryCostService<Integer> {

    @Override
    public Future<Integer> calc(QueryCostRequestContext context) {
        return Future.succeededFuture(1000);
    }

    @Override
    public Future<Integer> execute(QueryCostRequestContext context) {
        return Future.failedFuture(new DataSourceException("Unsupported operation"));
    }
}
