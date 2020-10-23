package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryCostService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service("adbQueryCostService")
public class AdbQueryCostService implements QueryCostService<Integer> {
    private final QueryEnrichmentService enrichmentService;

    @Override
    public void calc(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
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
    public void execute(QueryCostRequestContext context, Handler<AsyncResult<Integer>> handler) {
        handler.handle(Future.failedFuture(new RuntimeException("Unsupported operation")));
    }
}
