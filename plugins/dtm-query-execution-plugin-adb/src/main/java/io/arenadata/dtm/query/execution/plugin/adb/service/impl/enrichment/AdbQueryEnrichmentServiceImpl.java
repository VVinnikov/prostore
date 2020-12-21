package io.arenadata.dtm.query.execution.plugin.adb.service.impl.enrichment;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.AdbCalciteContextProvider;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.adb.service.SchemaExtender;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AdbQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final AdbCalciteContextProvider contextProvider;
    private final SchemaExtender schemaExtender;
    private final QueryParserService queryParserService;
    private final QueryGenerator adbQueryGenerator;

    @Autowired
    public AdbQueryEnrichmentServiceImpl(
            @Qualifier("adbCalciteDMLQueryParserService") QueryParserService queryParserService,
            AdbQueryGeneratorImpl adbQueryGeneratorimpl,
            AdbCalciteContextProvider contextProvider,
            @Qualifier("adbSchemaExtender") SchemaExtender schemaExtender) {
        this.queryParserService = queryParserService;
        this.adbQueryGenerator = adbQueryGeneratorimpl;
        this.contextProvider = contextProvider;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public Future<String> enrich(EnrichQueryRequest request) {
        return queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), request.getSchema()))
                .map(response -> {
                    contextProvider.enrichContext(response.getCalciteContext(),
                            generatePhysicalSchemas(request.getSchema()));
                    return response;
                })
                .compose(this::mutateQuery);
    }

    private Future<String> mutateQuery(QueryParserResponse response) {
        return Future.future(promise -> {
            adbQueryGenerator.mutateQuery(response.getRelNode(),
                    response.getQueryRequest().getDeltaInformations(),
                    response.getCalciteContext())
                    .onSuccess(result -> {
                        log.trace("Request generated: {}", result);
                        promise.complete(result);
                    })
                    .onFailure(promise::fail);
        });
    }

    private List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas) {
        return logicalSchemas.stream().map(schemaExtender::createPhysicalSchema).collect(Collectors.toList());
    }
}
