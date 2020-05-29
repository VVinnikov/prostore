package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.ParsedQueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

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
