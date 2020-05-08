package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.DownloadExtTableRecord;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;
import ru.ibs.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.MpprKafkaRequestFactoryImpl;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.EdmlService;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Обработка EDML-запроса перед передачей правильному плагину
 */
@Service("coreEdmlService")
public class EdmlServiceImpl implements EdmlService {
  private static final Logger LOGGER = LoggerFactory.getLogger(EdmlServiceImpl.class);

  private static final Pattern EXT_TABLE_AFTER_INSERT_INTO = Pattern.compile(
    ".*insert\\s+into\\s+([A-z.0-9]+)\\s+(select\\s+.*)",
    Pattern.CASE_INSENSITIVE
  );

  private final DataSourcePluginService pluginService;
  private final SchemaStorageProvider schemaStorageProvider;
  private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
  private final ServiceDao serviceDao;
  private final EdmlProperties edmlProperties;

  public EdmlServiceImpl(DataSourcePluginService pluginService,
                         ServiceDao serviceDao,
                         SchemaStorageProvider schemaStorageProvider,
                         EdmlProperties edmlProperties) {
    this.pluginService = pluginService;
    this.serviceDao = serviceDao;
    this.schemaStorageProvider = schemaStorageProvider;
    this.mpprKafkaRequestFactory = new MpprKafkaRequestFactoryImpl();
    this.edmlProperties = edmlProperties;
  }

  static String cutOutInsertInto(String sqlInsertSelect) {
    final Matcher cutOutInsertMatcher = EXT_TABLE_AFTER_INSERT_INTO.matcher(sqlInsertSelect);
    return cutOutInsertMatcher.matches() ? cutOutInsertMatcher.group(2) : null;
  }

  static String extractExternalTable(String sqlInsertSelect) {
    final Matcher afterInsertMatcher = EXT_TABLE_AFTER_INSERT_INTO.matcher(sqlInsertSelect);
    return afterInsertMatcher.matches() ? afterInsertMatcher.group(1) : null;
  }

  @Override
  public void execute(ParsedQueryRequest parsedQueryRequest, Handler<AsyncResult<QueryResult>> resultHandler) {
    schemaStorageProvider.getLogicalSchema(schemaAr -> {
      if (schemaAr.succeeded()) {
        JsonObject schema = schemaAr.result();
        executeWithSchema(schema, parsedQueryRequest, resultHandler);
      } else {
        resultHandler.handle(Future.failedFuture(schemaAr.cause()));
      }
    });
  }

  public void executeWithSchema(JsonObject schema, ParsedQueryRequest parsedQueryRequest, Handler<AsyncResult<QueryResult>> resultHandler) {
    LOGGER.debug("Начало обработки EDML-запроса. execute(type: {}, queryRequest: {})",
      parsedQueryRequest.getProcessingType(), parsedQueryRequest.getQueryRequest());
    final QueryRequest queryRequest = parsedQueryRequest.getQueryRequest();

    // В версии 2.1 внешняя таблица будет не только после INSERT INTO, но и в FROM/JOIN.
    // Придётся выбирать все таблицы и отсеивать внешние через таблицу download_external_table
    // Пока по-простому: таблица после INSERT INTO только одна.
    final String externalTable = extractExternalTable(queryRequest.getSql());
    String onlySelect = cutOutInsertInto(queryRequest.getSql());
    final QueryRequest qrOnlySelect = queryRequest.copy();
    qrOnlySelect.setSql(onlySelect);
    LOGGER.debug("От запроса оставили: {}", onlySelect);

    serviceDao.findDownloadExternalTable(qrOnlySelect.getDatamartMnemonic(), externalTable, ar -> {
      if (ar.succeeded()) {
        final DownloadExtTableRecord detRecord = ar.result();
        LOGGER.debug("Внешная таблица {} найдена", externalTable);
        final QueryExloadParam exloadParam = createQueryExloadParam(externalTable, qrOnlySelect, detRecord);
        serviceDao.insertDownloadQuery(exloadParam.getId(), detRecord.getId(), qrOnlySelect.getSql(), idqHandler -> {
          if (idqHandler.succeeded()) {
            if (Type.KAFKA_TOPIC == exloadParam.getLocationType()) {
              LOGGER.debug("Перед обращением к plugin.mmprKafka");
              pluginService.mpprKafka(
                edmlProperties.getSourceType(),
                mpprKafkaRequestFactory.create(qrOnlySelect, exloadParam, schema),
                resultHandler);
            } else {
              LOGGER.error("Другие типы ещё не реализованы");
              resultHandler.handle(Future.failedFuture("Другие типы ещё не реализованы"));
            }
          } else {
            resultHandler.handle(Future.failedFuture(ar.cause()));
          }
        });
      } else {
        resultHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @NotNull
  private QueryExloadParam createQueryExloadParam(String externalTable,
                                                  QueryRequest queryRequest,
                                                  DownloadExtTableRecord detRecord) {
    final QueryExloadParam exloadParam = new QueryExloadParam();
    exloadParam.setId(UUID.randomUUID());
    exloadParam.setDatamart(queryRequest.getDatamartMnemonic());
    exloadParam.setTableName(externalTable);
    exloadParam.setSqlQuery(queryRequest.getSql());
    exloadParam.setLocationType(detRecord.getLocationType());
    exloadParam.setLocationPath(detRecord.getLocationPath());
    exloadParam.setFormat(detRecord.getFormat());
    exloadParam.setChunkSize(detRecord.getChunkSize() != null ?
      detRecord.getChunkSize() : edmlProperties.getDefaultChunkSize());
    return exloadParam;
  }
}
