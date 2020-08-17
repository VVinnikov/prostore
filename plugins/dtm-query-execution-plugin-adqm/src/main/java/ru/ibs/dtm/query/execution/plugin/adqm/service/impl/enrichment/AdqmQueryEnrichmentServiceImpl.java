package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.QueryParserRequest;
import ru.ibs.dtm.common.dto.QueryParserResponse;
import ru.ibs.dtm.query.calcite.core.service.QueryParserService;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query.QueryRewriter;

@Service
@Slf4j
public class AdqmQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final QueryParserService queryParserService;
    private final AdqmCalciteContextProvider contextProvider;
    private final QueryRewriter queryRewriter;

    public AdqmQueryEnrichmentServiceImpl(@Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService,
                                          AdqmCalciteContextProvider contextProvider,
                                          QueryRewriter queryRewriter) {
        this.queryParserService = queryParserService;
        this.contextProvider = contextProvider;
        this.queryRewriter = queryRewriter;
    }

    @Override
    public void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> asyncHandler) {
        Future.future((Promise<QueryParserResponse> promise) ->
                queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), request.getSchema()), promise))
                .compose(parserResponse -> rewriteQuery(request, parserResponse))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        asyncHandler.handle(Future.succeededFuture(ar.result()));
                    } else {
                        asyncHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private Future<String> rewriteQuery(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        return Future.future((Promise<String> promise) -> {
                    try {
                        contextProvider.enrichContext(parserResponse.getCalciteContext(), request.getSchema());
                        // 2. Modify query - add filter for sys_from/sys_to columns based on deltas
                        // 3. Modify query - duplicate via union all (with sub queries) and rename table names to physical names
                        // 4. Modify query - rename schemas to physical name
                        queryRewriter.rewrite(request, parserResponse.getQueryRequest().getDeltaInformations(), promise);
                    } catch (Exception e) {
                        log.error("Error enrich calcite context by request: {}!", parserResponse, e);
                        promise.fail(e);
                    }
                }
        );
    }
}
