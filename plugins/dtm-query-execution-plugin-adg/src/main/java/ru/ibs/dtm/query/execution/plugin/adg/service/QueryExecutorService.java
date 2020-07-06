package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adg.model.QueryResultItem;

/**
 * Сервис исполнения запросов
 */
public interface QueryExecutorService {
  void execute(String sql, Handler<AsyncResult<QueryResultItem>> handler);

  Future<Object> executeProcedure(String procedure, Object... args);
}
