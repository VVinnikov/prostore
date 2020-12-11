package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.dml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

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
                adqmDatabaseExecutor.execute(sqlResult.result(), request.getMetadata(), executeResult -> {
                    if (executeResult.succeeded()) {
                        asyncHandler.handle(Future.succeededFuture(
                            QueryResult.builder()
                                .requestId(request.getQueryRequest().getRequestId())
                                .metadata(request.getMetadata())
                                .result(executeResult.result())
                                .build()
                        ));
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
