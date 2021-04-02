package io.arenadata.dtm.query.execution.plugin.adg.base.service.client;

import io.vertx.core.Future;

import java.util.List;

/**
 * Tarantool client
 */
public interface AdgClient {
    void close();

    Future<List<Object>> eval(String expression, Object... args);

    Future<List<Object>> call(String function, Object... args);

    Future<List<Object>> callQuery(String sql, Object... params);

    Future<List<Object>> callLoadLines(String table, Object... rows);

    boolean isAlive();
}
