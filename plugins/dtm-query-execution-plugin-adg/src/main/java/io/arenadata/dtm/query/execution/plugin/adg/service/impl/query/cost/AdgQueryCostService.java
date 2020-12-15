package io.arenadata.dtm.query.execution.plugin.adg.service.impl.query.cost;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryCostService;
import org.springframework.stereotype.Service;

@Service("adgQueryCostService")
public class AdgQueryCostService implements QueryCostService<Integer> {

    @Override
    public void calc(QueryCostRequestContext context, AsyncHandler<Integer> handler) {
        handler.handleSuccess(1000);
    }

    @Override
    public void execute(QueryCostRequestContext context, AsyncHandler<Integer> handler) {
        handler.handleError(new DataSourceException("Unsupported operation"));
    }
}
