package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;

/**
 * Диспетчер запросов
 */
public interface QueryDispatcher {

  /**
   * <p>Направить на исполнение</p>
   *
   * @param parsedQueryRequest запрос с дополнительной информации
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void dispatch(ParsedQueryRequest parsedQueryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler);
}
