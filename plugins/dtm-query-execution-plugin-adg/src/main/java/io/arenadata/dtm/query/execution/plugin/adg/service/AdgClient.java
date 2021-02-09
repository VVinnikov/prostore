package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.vertx.core.Future;

import java.util.List;

/**
 * Tarantool client
 */
public interface AdgClient {
    void close();

    Future<List<?>> eval(String expression, Object... args);

    Future<List<?>> call(String function, Object... args);

    Future<List<?>> callQuery(String sql, Object... params);

    Future<List<?>> callLoadLines(String table, Object... rows);

    boolean isAlive();
}
