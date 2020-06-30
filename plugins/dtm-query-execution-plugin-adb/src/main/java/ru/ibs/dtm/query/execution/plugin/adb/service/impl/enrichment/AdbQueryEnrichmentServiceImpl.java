package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.dto.RegexPreprocessorResult;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryGenerator;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryParserService;
import ru.ibs.dtm.query.execution.plugin.adb.service.SchemaExtender;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.QueryRegexPreprocessor;

@Service
@Slf4j
public class AdbQueryEnrichmentServiceImpl implements QueryEnrichmentService {
  private QueryParserService queryParserService;
  private QueryGenerator adbQueryGenerator;
  private final DeltaService deltaService;
  private final CalciteContextProvider contextProvider;
  private final QueryRegexPreprocessor regexPreprocessor;
  private final SchemaExtender schemaExtender;

  public AdbQueryEnrichmentServiceImpl(QueryParserService queryParserService,
                                       AdbQueryGeneratorImpl adbQueryGeneratorimpl,
                                       DeltaService deltaService,
                                       CalciteContextProvider contextProvider,
                                       QueryRegexPreprocessor regexPreprocessor,
                                       @Qualifier("adbSchemaExtender") SchemaExtender schemaExtender) {
    this.queryParserService = queryParserService;
    this.adbQueryGenerator = adbQueryGeneratorimpl;
    this.deltaService = deltaService;
    this.contextProvider = contextProvider;
    this.regexPreprocessor = regexPreprocessor;
    this.schemaExtender = schemaExtender;
  }

  @Override
  public void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> asyncHandler) {
    //Вырезаем дату-время systemDateTime из запроса
    regexPreprocessor.process(request.getQueryRequest(), ar -> {
      if (ar.succeeded()) {
        // на основе даты определяем номер дельты
        RegexPreprocessorResult regexPreprocessorResult = ar.result();
        calculateDeltaValue(request.getQueryRequest(), regexPreprocessorResult.getSystemTimeAsOf(), deltaResult -> {
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
          CalciteContext calciteContext = contextProvider.context(logicalSchema);
          // парсим исходный SQL в RelNode
          queryParserService.parse(regexPreprocessorResult.getActualQueryRequest(), calciteContext, parsedQueryResult -> {
            if (parsedQueryResult.succeeded()) {
              contextProvider.enrichContext(calciteContext, schemaExtender.generatePhysicalSchema(logicalSchema));
              // формируем новый sql-запрос
              adbQueryGenerator.mutateQuery(parsedQueryResult.result(), deltaResult.result(), calciteContext, enrichedQueryResult -> {
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

  private void calculateDeltaValue(QueryRequest request, String selectOn, Handler<AsyncResult<Long>> handler) {
    String datamartMnemonic = request.getDatamartMnemonic();
    ActualDeltaRequest deltaRequest = new ActualDeltaRequest(datamartMnemonic, selectOn);

    deltaService.getDeltaOnDateTime(deltaRequest, deltaResult -> {
      if (deltaResult.succeeded()) {
        handler.handle(Future.succeededFuture(deltaResult.result()));
      } else {
        log.error("Не удалось получить дельту витрины {} на дату {}", datamartMnemonic, selectOn, deltaResult.cause());
        handler.handle(Future.failedFuture(deltaResult.cause()));
      }
    });
  }
}
