package ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.cost.QueryCostAlgorithm;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.AdbQueryCostAnalyzer;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.QueryCostRequest;


@Slf4j
@Component
@RequiredArgsConstructor
public class DelayInSecondsAdbQueryCostAnalyzer implements AdbQueryCostAnalyzer<Integer> {
    private final QueryEnrichmentService enrichmentService;

    @Override
    public void analyze(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
        val request = (QueryCostRequest) context.getRequest();
        val enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema());
        enrichmentService.enrich(enrichQueryRequest, ar -> {
            if (ar.succeeded()) {
                log.debug("QueryCostRequest enrich completed: [{}]", ar.result());
                handler.handle(Future.succeededFuture(0));
            } else {
                log.error("QueryCost analyzing error", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public QueryCostAlgorithm getAlgorithm() {
        return QueryCostAlgorithm.DELAY_IN_SECONDS;
    }
}
