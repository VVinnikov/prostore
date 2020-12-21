package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.AdqmCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adqm.service.SchemaExtender;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
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
    public Future<String> enrich(EnrichQueryRequest request) {
        return queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), request.getSchema()))
                .compose(parserResponse -> modifyQuery(parserResponse, request));
    }

    private Future<String> modifyQuery(QueryParserResponse parserResponse, EnrichQueryRequest request) {
        return Future.future(promise -> {
            contextProvider.enrichContext(parserResponse.getCalciteContext(),
                    generatePhysicalSchema(request.getSchema(), request.getQueryRequest()));
            // form a new sql query
            adqmQueryGenerator.mutateQuery(parserResponse.getRelNode(),
                    parserResponse.getQueryRequest().getDeltaInformations(),
                    parserResponse.getCalciteContext(),
                    request.getQueryRequest(),
                    request.isLocal())
                    .onComplete(enrichedQueryResult -> {
                        if (enrichedQueryResult.succeeded()) {
                            log.debug("Request generated: {}", enrichedQueryResult.result());
                            promise.complete(enrichedQueryResult.result());
                        } else {
                            promise.fail(enrichedQueryResult.cause());
                        }
                    });
        });
    }

    private List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, QueryRequest request) {
        return logicalSchemas.stream()
                .map(ls -> schemaExtender.createPhysicalSchema(ls, request.getEnvName()))
                .collect(Collectors.toList());
    }
}
