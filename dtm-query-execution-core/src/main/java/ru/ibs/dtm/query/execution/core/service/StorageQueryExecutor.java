package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Сервис исполнения запросов
 */
public interface StorageQueryExecutor {
  void execute(String sql, Handler<AsyncResult<List<List<?>>>> resultHandler);
  void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler);
  void executeWithParams(String sql, List<Object> params, Handler<AsyncResult<?>> resultHandler);
}
