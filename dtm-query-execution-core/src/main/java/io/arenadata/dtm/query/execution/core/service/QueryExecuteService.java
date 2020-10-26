package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.ParsedQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Сервис выполнения SQL+
 */
public interface QueryExecuteService {

  /**
   * <p>Выполнить запрос</p>
   *
   * @param parsedQueryRequest предобработанный запрос
   * @param asyncResultHandler хэндлер асинхронной обработки результата
   */
  void execute(ParsedQueryRequest parsedQueryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler);

  /**
   * <p>Получить тип обрабатываемых выражений</p>
   *
   * @return тип обрабатываемых выражений
   */
  SqlProcessingType getSqlProcessingType();
}
