package io.arenadata.dtm.query.execution.core.query.executor;

import io.vertx.core.Future;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;

public interface QueryExecutor {

    Future<UpdateResult> executeUpdate(String datamartMnemonic, String sql);

    Future<ResultSet> executeQuery(String datamartMnemonic, String sql);

    Future<ResultSet> executeQuery(String sql);
}
