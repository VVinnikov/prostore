package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

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
import ru.ibs.dtm.query.execution.plugin.adg.calcite.AdgCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.schema.SchemaDescription;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryGenerator;
import ru.ibs.dtm.query.execution.plugin.adg.service.SchemaExtender;

@Service
@Slf4j
public class AdgQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final AdgCalciteContextProvider contextProvider;
    private final QueryParserService queryParserService;
    private final QueryGenerator adgQueryGenerator;
    private final SchemaExtender schemaExtender;

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
    public void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> asyncHandler) {
        //FIXME исправить после реализации использования нескольких схем
        Datamart logicalSchema = request.getSchema().get(0);
        queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), request.getSchema().get(0)), ar -> {
            if (ar.succeeded()) {
                val parserResponse = ar.result();
                contextProvider.enrichContext(parserResponse.getCalciteContext(), logicalSchema);
                val schemaDescription = new SchemaDescription();
                schemaDescription.setLogicalSchema(logicalSchema);
                val physicalSchema = schemaExtender.generatePhysicalSchema(logicalSchema, parserResponse.getQueryRequest());
                contextProvider.enrichContext(parserResponse.getCalciteContext(), physicalSchema);
                // формируем новый sql-запрос
                adgQueryGenerator.mutateQuery(parserResponse.getRelNode(),
                        parserResponse.getQueryRequest().getDeltaInformations(),
                        schemaDescription,
                        parserResponse.getCalciteContext(),
                        request.getQueryRequest(),
                        enrichedQueryResult -> {
                            if (enrichedQueryResult.succeeded()) {
                                log.info("Сформирован запрос: {}", enrichedQueryResult.result());
                                asyncHandler.handle(Future.succeededFuture(enrichedQueryResult.result()));
                            } else {
                                log.debug("Ошибка при обогащении запроса", enrichedQueryResult.cause());
                                asyncHandler.handle(Future.failedFuture(enrichedQueryResult.cause()));
                            }
                        });
            } else {
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
