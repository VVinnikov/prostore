package ru.ibs.dtm.query.execution.plugin.adqm.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;

import java.util.List;
import java.util.Map;

/**
 * Сервис исполнения запросов
 */
public interface DatabaseExecutor {
    void execute(String sql, Handler<AsyncResult<JsonArray>> resultHandler);

    void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler);

    void executeWithParams(String sql, List<Object> params, Handler<AsyncResult<?>> resultHandler);
}
