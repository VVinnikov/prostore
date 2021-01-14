package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryCostService;
import io.vertx.core.Future;
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
    public Future<Integer> calc(QueryCostRequestContext context) {
        return Future.future(promise -> {
            val request = (QueryCostRequest) context.getRequest();
            val enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(),
                    request.getSchema(),
                    context.getQuery());
            enrichmentService.enrich(enrichQueryRequest)
                    .onSuccess(result -> {
                        log.debug("QueryCostRequest enrich completed: [{}]", result);
                        promise.complete(0);
                    })
                    .onFailure(promise::fail);
        });
    }

    @Override
    public Future<Integer> execute(QueryCostRequestContext context) {
        return Future.failedFuture(new DataSourceException("Unsupported operation"));
    }
}
