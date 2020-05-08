package ru.ibs.dtm.query.execution.plugin.adb.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;
import java.util.Map;

/**
 * Сервис исполнения запросов
 */
public interface DatabaseExecutor {
  void execute(String sql, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler);
  void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler);
  void executeWithParams(String sql, List<Object> params, Handler<AsyncResult<?>> resultHandler);
}
