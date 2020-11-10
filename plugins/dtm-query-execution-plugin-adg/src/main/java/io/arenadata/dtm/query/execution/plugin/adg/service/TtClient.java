package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Клиент Tarantool
 */
public interface TtClient {
  void close();
  void eval(Handler<AsyncResult<List<?>>> handler, String expression, Object... args);
  void call(Handler<AsyncResult<List<?>>> handler, String function, Object... args);
  void callQuery(Handler<AsyncResult<List<?>>> handler, String sql, Object... params);
  void callLoadLines(Handler<AsyncResult<List<?>>> handler, String table, Object... rows);
  boolean isAlive();
}
