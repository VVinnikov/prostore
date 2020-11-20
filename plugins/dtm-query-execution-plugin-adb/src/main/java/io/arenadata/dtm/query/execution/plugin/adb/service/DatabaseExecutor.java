package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Сервис исполнения запросов
 */
public interface DatabaseExecutor {
  void execute(String sql, List<ColumnMetadata> metadata, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler);
  void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler);
  void executeWithParams(String sql, List<Object> params, List<ColumnMetadata> metadata, Handler<AsyncResult<?>> resultHandler);
  void executeInTransaction(List<PreparedStatementRequest> requests, Handler<AsyncResult<Void>> resultHandler);

  default Future<List<Map<String, Object>>> execute(String sql) {
    return Future.future(promise -> execute(sql, Collections.emptyList(), promise));
  }
}
