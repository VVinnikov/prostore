package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

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
import ru.ibs.dtm.query.execution.plugin.adb.calcite.AdbCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryGenerator;
import ru.ibs.dtm.query.execution.plugin.adb.service.SchemaExtender;

@Service
@Slf4j
public class AdbQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private final AdbCalciteContextProvider contextProvider;
    private final SchemaExtender schemaExtender;
    private final QueryParserService queryParserService;
    private final QueryGenerator adbQueryGenerator;

    public AdbQueryEnrichmentServiceImpl(
            @Qualifier("adbCalciteDMLQueryParserService") QueryParserService queryParserService,
            AdbQueryGeneratorImpl adbQueryGeneratorimpl,
            AdbCalciteContextProvider contextProvider,
            @Qualifier("adbSchemaExtender") SchemaExtender schemaExtender
    ) {
        this.queryParserService = queryParserService;
        this.adbQueryGenerator = adbQueryGeneratorimpl;
        this.contextProvider = contextProvider;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> asyncHandler) {
        //FIXME исправить после реализации использования нескольких схем
        Datamart logicalSchema = request.getSchema().get(0);
        queryParserService.parse(new QueryParserRequest(request.getQueryRequest(), logicalSchema), ar -> {
            if (ar.succeeded()) {
                val parserResponse = ar.result();
                contextProvider.enrichContext(parserResponse.getCalciteContext(), schemaExtender.generatePhysicalSchema(logicalSchema));
                // формируем новый sql-запрос
                adbQueryGenerator.mutateQuery(parserResponse.getRelNode(),
                        parserResponse.getQueryRequest().getDeltaInformations(),
                        parserResponse.getCalciteContext(),
                        enrichedQueryResult -> {
                            if (enrichedQueryResult.succeeded()) {
                                log.trace("Request generated: {}", enrichedQueryResult.result());
                                asyncHandler.handle(Future.succeededFuture(enrichedQueryResult.result()));
                            } else {
                                log.debug("Error while enriching request", enrichedQueryResult.cause());
                                asyncHandler.handle(Future.failedFuture(enrichedQueryResult.cause()));
                            }
                        });
            } else {
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }
        });

    }
}
