package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.QueryParserRequest;
import ru.ibs.dtm.query.calcite.core.service.QueryParserService;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
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
        // FIXME rewrite to the Future's compose instead of callback hell
        //FIXME исправить после реализации использования нескольких схем
        Datamart logicalSchema = request.getSchema().get(0);
        queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), logicalSchema), ar -> {
            if (ar.succeeded()) {
                val parserResponse = ar.result();
                contextProvider.enrichContext(parserResponse.getCalciteContext(), logicalSchema);

                // 2. Modify query - add filter for sys_from/sys_to columns based on deltas
                // 3. Modify query - duplicate via union all (with sub queries) and rename table names to physical names
                // 4. Modify query - rename schemas to physical name
                queryRewriter.rewrite(request.getQueryRequest().getSql(), parserResponse.getQueryRequest().getDeltaInformations(), ar3 -> {
                    if (ar3.failed()) {
                        asyncHandler.handle(Future.failedFuture(ar3.cause()));
                    }

                    asyncHandler.handle(Future.succeededFuture(ar3.result()));
                });

            } else {
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
