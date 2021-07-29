package io.arenadata.dtm.query.execution.plugin.adb.enrichment.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.service.AdbCalciteContextProvider;
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

@Service("adbQueryEnrichmentService")
@Slf4j
public class AdbQueryEnrichmentService implements QueryEnrichmentService {
    private final AdbCalciteContextProvider contextProvider;
    private final SchemaExtender schemaExtender;
    private final QueryGenerator adbQueryGenerator;

    @Autowired
    public AdbQueryEnrichmentService(
            @Qualifier("adbQueryGenerator") QueryGenerator adbQueryGenerator,
            AdbCalciteContextProvider contextProvider,
            @Qualifier("adbSchemaExtender") SchemaExtender schemaExtender) {
        this.adbQueryGenerator = adbQueryGenerator;
        this.contextProvider = contextProvider;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request, QueryParserResponse parserResponse) {
        contextProvider.enrichContext(parserResponse.getCalciteContext(),
                generatePhysicalSchemas(request.getSchema()));
        return mutateQuery(parserResponse, request);
    }

    private Future<String> mutateQuery(QueryParserResponse response, EnrichQueryRequest request) {
        return Future.future(promise -> adbQueryGenerator.mutateQuery(response.getRelNode(),
                request.getDeltaInformations(),
                response.getCalciteContext(),
                null)
                .onSuccess(result -> {
                    log.trace("Request generated: {}", result);
                    promise.complete(result);
                })
                .onFailure(promise::fail));
    }

    private List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas) {
        return logicalSchemas.stream()
                .map(logicalSchema -> schemaExtender.createPhysicalSchema(logicalSchema, ""))
                .collect(Collectors.toList());
    }
}
