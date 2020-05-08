package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.AdgCalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.RegexPreprocessorResult;
import ru.ibs.dtm.query.execution.plugin.adg.dto.schema.SchemaDescription;
import ru.ibs.dtm.query.execution.plugin.adg.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryGenerator;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryParserService;
import ru.ibs.dtm.query.execution.plugin.adg.service.impl.query.AdgQueryRegexPreprocessor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AdgQueryEnrichmentServiceImpl implements QueryEnrichmentService {
  private final DeltaService deltaService;
  private final AdgQueryRegexPreprocessor regexPreprocessor;
  private final AdgCalciteContextProvider contextProvider;
  private final QueryParserService queryParserService;
  private final QueryGenerator adgQueryGenerator;

  public AdgQueryEnrichmentServiceImpl(
    DeltaService deltaService,
    AdgQueryRegexPreprocessor regexPreprocessor,
    AdgCalciteContextProvider contextProvider,
    @Qualifier("adgCalciteDmlQueryParserService") QueryParserService queryParserService,
    @Qualifier("adgQueryGenerator") QueryGenerator adgQueryGenerator) {
    this.deltaService = deltaService;
    this.regexPreprocessor = regexPreprocessor;
    this.contextProvider = contextProvider;
    this.queryParserService = queryParserService;
    this.adgQueryGenerator = adgQueryGenerator;
  }

  @Override
  public void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> asyncHandler) {
    //Вырезаем дату-время systemDateTime всех таблиц из запроса, складывая их в мапу таблица -> дата-время
    regexPreprocessor.process(request.getQueryRequest(), ar -> {
      if (ar.succeeded()) {
        // на основе дат-времён определяем номера дельт
        RegexPreprocessorResult regexPreprocessorResult = ar.result();
        final Map<String, String> systemTimesForTables = regexPreprocessorResult.getSystemTimesForTables();
        calculateDeltaValues(request.getQueryRequest(), systemTimesForTables, deltaResult -> {
          if (deltaResult.failed()) {
            asyncHandler.handle(Future.failedFuture(deltaResult.cause()));
            return;
          }
          Datamart logicalSchema;
          try {
            logicalSchema = request.getSchema().mapTo(Datamart.class);
          } catch (Exception ex) {
            log.error("Ошибка десериализации схемы");
            asyncHandler.handle(Future.failedFuture(ex));
            return;
          }
          SchemaDescription schemaDescription = new SchemaDescription();
          schemaDescription.setLogicalSchema(logicalSchema);

          CalciteContext calciteContext = contextProvider.context();

          // парсим исходный SQL в RelNode
          queryParserService.parse(regexPreprocessorResult.getActualQueryRequest(), schemaDescription, calciteContext, parsedQueryResult -> {
            if (parsedQueryResult.succeeded()) {
              // формируем новый sql-запрос
              adgQueryGenerator.mutateQuery(parsedQueryResult.result(), deltaResult.result(), schemaDescription, calciteContext, enrichedQueryResult -> {
                if (enrichedQueryResult.succeeded()) {
                  log.trace("Сформирован запрос: {}", enrichedQueryResult.result());
                  asyncHandler.handle(Future.succeededFuture(enrichedQueryResult.result()));
                } else {
                  log.debug("Ошибка при обогащении запроса", enrichedQueryResult.cause());
                  asyncHandler.handle(Future.failedFuture(enrichedQueryResult.cause()));
                }
              });
            } else {
              asyncHandler.handle(Future.failedFuture(parsedQueryResult.cause()));
            }
          });
        });
      } else {
        log.error("Ошибка предобработки запроса", ar.cause());
        asyncHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void calculateDeltaValues(QueryRequest request, Map<String, String> tableToDateTime, Handler<AsyncResult<List<Long>>> handler) {
    String datamartMnemonic = request.getDatamartMnemonic();
    final List<ActualDeltaRequest> deltaRequests =
      tableToDateTime.values().stream()
        .map(dateTime -> new ActualDeltaRequest(datamartMnemonic, dateTime))
        .collect(Collectors.toList());
    deltaService.getDeltasOnDateTimes(deltaRequests, handler);
  }
}
