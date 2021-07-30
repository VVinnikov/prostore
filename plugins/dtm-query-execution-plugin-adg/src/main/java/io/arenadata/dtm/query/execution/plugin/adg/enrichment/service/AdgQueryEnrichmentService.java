package io.arenadata.dtm.query.execution.plugin.adg.enrichment.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.service.AdgCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.SchemaExtender;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("adgQueryEnrichmentService")
@Slf4j
public class AdgQueryEnrichmentService implements QueryEnrichmentService {
    private final AdgCalciteContextProvider contextProvider;
    private final QueryGenerator adgQueryGenerator;
    private final SchemaExtender schemaExtender;

    @Autowired
    public AdgQueryEnrichmentService(
            AdgCalciteContextProvider contextProvider,
            @Qualifier("adgQueryGenerator") QueryGenerator adgQueryGenerator,
            SchemaExtender adgSchemaExtender) {
        this.contextProvider = contextProvider;
        this.adgQueryGenerator = adgQueryGenerator;
        this.schemaExtender = adgSchemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        return modifyQuery(parserResponse, request);
    }

    private Future<String> modifyQuery(QueryParserResponse parsedQuery,
                                       EnrichQueryRequest request) {
        return Future.future(promise -> {
            contextProvider.enrichContext(parsedQuery.getCalciteContext(),
                    generatePhysicalSchema(request.getSchema(), request.getEnvName()));
            // form a new sql query
            adgQueryGenerator.mutateQuery(parsedQuery.getRelNode(),
                    request.getDeltaInformations(),
                    parsedQuery.getCalciteContext(),
                    request)
                    .onSuccess(enrichResult -> {
                        log.debug("Request generated: {}", enrichResult);
                        promise.complete(enrichResult);
                    })
                    .onFailure(promise::fail);
        });
    }

    private List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, String envName) {
        return logicalSchemas.stream()
                .map(ls -> schemaExtender.createPhysicalSchema(ls, envName))
                .collect(Collectors.toList());
    }
}
