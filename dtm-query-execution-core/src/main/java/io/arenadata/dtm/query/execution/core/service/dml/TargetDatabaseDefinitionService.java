package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

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
  void getTargetSource(QuerySourceRequest request, Handler<AsyncResult<QuerySourceRequest>> handler);
}
