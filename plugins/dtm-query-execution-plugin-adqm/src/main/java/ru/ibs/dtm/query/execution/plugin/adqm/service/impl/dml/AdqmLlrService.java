package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;

@Service("adqmLlrService")
@Slf4j
public class AdqmLlrService implements LlrService<QueryResult> {

    private final QueryEnrichmentService adqmQueryEnrichmentService;
    private final DatabaseExecutor adqmDatabaseExecutor;

    public AdqmLlrService(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService adqmQueryEnrichmentService,
                          @Qualifier("adqmQueryExecutor") DatabaseExecutor adqmDatabaseExecutor) {
        this.adqmQueryEnrichmentService = adqmQueryEnrichmentService;
        this.adqmDatabaseExecutor = adqmDatabaseExecutor;
    }

    @Override
    public void execute(LlrRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
        LlrRequest request = context.getRequest();
        EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema());
        adqmQueryEnrichmentService.enrich(enrichQueryRequest, sqlResult -> {
            if (sqlResult.succeeded()) {
                adqmDatabaseExecutor.execute(sqlResult.result(), executeResult -> {
                    if (executeResult.succeeded()) {
                        QueryResult queryResult = QueryResult.emptyResult();
                        queryResult.setRequestId(request.getQueryRequest().getRequestId());
                        queryResult.setResult(executeResult.result());
                        asyncHandler.handle(Future.succeededFuture(queryResult));
                    } else {
                        asyncHandler.handle(Future.failedFuture(executeResult.cause()));
                    }
                });
            } else {
                log.error("Error while enriching request");
                asyncHandler.handle(Future.failedFuture(sqlResult.cause()));
            }
        });
    }
}
