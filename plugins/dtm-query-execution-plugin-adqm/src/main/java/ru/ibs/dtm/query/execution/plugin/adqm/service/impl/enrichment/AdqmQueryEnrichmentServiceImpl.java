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
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryGenerator;
import ru.ibs.dtm.query.execution.plugin.adqm.service.SchemaExtender;

@Slf4j
@Service("adqmQueryEnrichmentService")
public class AdqmQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final AdqmCalciteContextProvider contextProvider;
    private final QueryParserService queryParserService;
    private final QueryGenerator adqmQueryGenerator;
    private final SchemaExtender schemaExtender;

    public AdqmQueryEnrichmentServiceImpl(
        @Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService,
        AdqmCalciteContextProvider contextProvider,
        @Qualifier("adqmQueryGenerator") QueryGenerator adqmQueryGenerator, SchemaExtender schemaExtender) {
        this.contextProvider = contextProvider;
        this.queryParserService = queryParserService;
        this.adqmQueryGenerator = adqmQueryGenerator;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> asyncHandler) {
        queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), request.getSchema()), ar -> {
            if (ar.succeeded()) {
                val parserResponse = ar.result();
                contextProvider.enrichContext(parserResponse.getCalciteContext(),
                    schemaExtender.generatePhysicalSchema(request.getSchema(), request.getQueryRequest()));
                // формируем новый sql-запрос
                adqmQueryGenerator.mutateQuery(parserResponse.getRelNode(),
                    parserResponse.getQueryRequest().getDeltaInformations(),
                    parserResponse.getCalciteContext(),
                    request.getQueryRequest(),
                    enrichedQueryResult -> {
                        if (enrichedQueryResult.succeeded()) {
                            log.debug("Request generated: {}", enrichedQueryResult.result());
                            asyncHandler.handle(Future.succeededFuture(enrichedQueryResult.result()));
                        } else {
                            log.error("Error while enriching request", enrichedQueryResult.cause());
                            asyncHandler.handle(Future.failedFuture(enrichedQueryResult.cause()));
                        }
                    });
            } else {
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
