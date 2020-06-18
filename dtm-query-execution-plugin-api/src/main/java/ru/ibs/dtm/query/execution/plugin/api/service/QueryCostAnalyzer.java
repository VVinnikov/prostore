package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import ru.ibs.dtm.common.cost.QueryCostAlgorithm;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;

public interface QueryCostAnalyzer<T, S extends QueryCostService<T>> {
    void analyze(QueryCostRequestContext context, Handler<AsyncResult<T>> handler);

    @Autowired
    default void registration(S service) {
        service.addAnalyzer(this);
    }

    QueryCostAlgorithm getAlgorithm();
}
