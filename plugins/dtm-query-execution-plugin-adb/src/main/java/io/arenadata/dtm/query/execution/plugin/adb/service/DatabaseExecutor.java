package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;

/**
 * Database query execution service
 */
public interface DatabaseExecutor {

    Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata);

    Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata);

    Future<List<Map<String, Object>>> executeWithParams(String sql, QueryParameters params, List<ColumnMetadata> metadata);

    Future<Void> executeUpdate(String sql);

    Future<Void> executeInTransaction(List<PreparedStatementRequest> requests);

    default Future<List<Map<String, Object>>> execute(String sql) {
        return execute(sql, null);
    }
}
