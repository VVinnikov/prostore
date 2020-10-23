package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;
import java.util.Map;

/**
 * Сервис исполнения запросов
 */
public interface QueryExecutorService {
    void execute(String sql, List<ColumnMetadata> metadata, Handler<AsyncResult<List<Map<String, Object>>>> handler);

    Future<Object> executeProcedure(String procedure, Object... args);
}
