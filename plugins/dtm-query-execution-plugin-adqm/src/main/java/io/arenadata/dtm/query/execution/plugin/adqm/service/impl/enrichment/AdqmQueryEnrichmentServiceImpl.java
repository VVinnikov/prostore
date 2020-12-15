package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adqm.service.SchemaExtender;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service("adqmQueryEnrichmentService")
public class AdqmQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final AdqmCalciteContextProvider contextProvider;
    private final QueryParserService queryParserService;
    private final QueryGenerator adqmQueryGenerator;
    private final SchemaExtender schemaExtender;

    @Autowired
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
    public void enrich(EnrichQueryRequest request, AsyncHandler<String> handler) {
        queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), request.getSchema()), ar -> {
            if (ar.succeeded()) {
                val parserResponse = ar.result();
                contextProvider.enrichContext(parserResponse.getCalciteContext(),
                    generatePhysicalSchema(request.getSchema(), request.getQueryRequest()));
                // form a new sql query
                adqmQueryGenerator.mutateQuery(parserResponse.getRelNode(),
                    parserResponse.getQueryRequest().getDeltaInformations(),
                    parserResponse.getCalciteContext(),
                    request.getQueryRequest(),
                    request.isLocal(),
                    enrichedQueryResult -> {
                        if (enrichedQueryResult.succeeded()) {
                            log.debug("Request generated: {}", enrichedQueryResult.result());
                            handler.handle(Future.succeededFuture(enrichedQueryResult.result()));
                        } else {
                            handler.handle(Future.failedFuture(enrichedQueryResult.cause()));
                        }
                    });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, QueryRequest request) {
        return logicalSchemas.stream()
                .map(ls -> schemaExtender.createPhysicalSchema(ls, request.getEnvName()))
                .collect(Collectors.toList());
    }
}
