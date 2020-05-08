package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;

/**
 * Сервис оперделения целевой субд
 */
public interface TargetDatabaseDefinitionService {

  /**
   * Получить тип целевого ресурса
   *
   * @param request запрос
   * @return запрос с определенным типом
   */
  void getTargetSource(QueryRequest request, Handler<AsyncResult<QuerySourceRequest>> handler);
}
