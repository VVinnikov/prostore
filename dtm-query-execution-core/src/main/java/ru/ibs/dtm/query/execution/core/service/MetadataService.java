package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;

/**
 * Сервис выполнения запросов к информационной схеме
 */
public interface MetadataService {

  void executeQuery(QueryRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler);
}
