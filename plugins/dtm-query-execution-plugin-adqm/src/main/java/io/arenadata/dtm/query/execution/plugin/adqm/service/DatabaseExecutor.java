package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;

import java.util.List;
import java.util.Map;

/**
 * Query execution service
 */
public interface DatabaseExecutor {
    void execute(String sql, List<ColumnMetadata> metadata, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler);

    void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler);

    void executeWithParams(String sql, List<Object> params, List<ColumnMetadata> metadata, Handler<AsyncResult<?>> resultHandler);

    default Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        Promise<List<Map<String, Object>>> p = Promise.promise();
        execute(sql, metadata, p);
        return p.future();
    }

    default Future<Void> executeUpdate(String sql) {
        Promise<Void> p = Promise.promise();
        executeUpdate(sql, p);
        return p.future();
    }
}
