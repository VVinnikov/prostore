package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;
import java.util.Map;

/**
 * Query execution service
 */
public interface QueryExecutorService {
    Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata);

    Future<Object> executeProcedure(String procedure, Object... args);
}
