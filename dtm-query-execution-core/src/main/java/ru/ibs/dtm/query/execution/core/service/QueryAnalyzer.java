package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;

/**
 * Сервис анализа запросов
 */
public interface QueryAnalyzer {

  /**
   * <p>Проанализировать и выполнить запрос</p>
   *
   * @param queryRequest       запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void analyzeAndExecute(QueryRequest queryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler);
}
