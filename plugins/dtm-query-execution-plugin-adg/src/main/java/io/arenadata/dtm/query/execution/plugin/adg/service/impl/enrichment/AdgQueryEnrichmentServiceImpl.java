package io.arenadata.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.calcite.AdgCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adg.service.SchemaExtender;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AdgQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final AdgCalciteContextProvider contextProvider;
    private final QueryParserService queryParserService;
    private final QueryGenerator adgQueryGenerator;
    private final SchemaExtender schemaExtender;

    @Autowired
    public AdgQueryEnrichmentServiceImpl(
            @Qualifier("adgCalciteDMLQueryParserService") QueryParserService queryParserService,
            AdgCalciteContextProvider contextProvider,
            @Qualifier("adgQueryGenerator") QueryGenerator adgQueryGenerator, SchemaExtender schemaExtender) {
        this.contextProvider = contextProvider;
        this.queryParserService = queryParserService;
        this.adgQueryGenerator = adgQueryGenerator;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request) {
        return queryParserService.parse(new QueryParserRequest(request.getQuery(), request.getSchema()))
                .compose(parsedQuery -> modifyQuery(parsedQuery, request));
    }

    private Future<String> modifyQuery(QueryParserResponse parsedQuery,
                                       EnrichQueryRequest request) {
        return Future.future(promise -> {
            contextProvider.enrichContext(parsedQuery.getCalciteContext(),
                    generatePhysicalSchema(request.getSchema(), request.getQueryRequest()));
            // form a new sql query
            adgQueryGenerator.mutateQuery(parsedQuery.getRelNode(),
                    request.getQueryRequest().getDeltaInformations(),
                    parsedQuery.getCalciteContext(),
                    request.getQueryRequest())
                    .onSuccess(enrichResult -> {
                        log.debug("Request generated: {}", enrichResult);
                        promise.complete(enrichResult);
                    })
                    .onFailure(promise::fail);
        });
    }

    private List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, QueryRequest request) {
        return logicalSchemas.stream()
                .map(ls -> schemaExtender.createPhysicalSchema(ls, request.getEnvName()))
                .collect(Collectors.toList());
    }
}
