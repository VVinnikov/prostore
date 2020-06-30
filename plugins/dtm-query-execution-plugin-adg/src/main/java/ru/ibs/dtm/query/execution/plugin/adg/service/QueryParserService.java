package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.calcite.rel.RelRoot;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.dto.schema.SchemaDescription;

/**
 * Сервис парсинга запроса
 */
public interface QueryParserService {
  void parse(QueryRequest querySourceRequest, SchemaDescription schemaDescription, CalciteContext calciteContext, Handler<AsyncResult<RelRoot>> asyncResultHandler);
}
