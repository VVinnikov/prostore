package ru.ibs.dtm.query.execution.plugin.adb.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.rel.RelRoot;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.CalciteContext;

/**
 * Сервис парсинга запроса
 */
public interface QueryParserService {
  void parse(QueryRequest querySourceRequest, CalciteContext calciteContext, Handler<AsyncResult<RelRoot>> asyncResultHandler);
}
