package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.rel.RelRoot;

/**
 * Сервис парсинга запроса
 */
public interface QueryParserService {
  void parse(QueryRequest querySourceRequest, CalciteContext calciteContext, Handler<AsyncResult<RelRoot>> asyncResultHandler);
}
