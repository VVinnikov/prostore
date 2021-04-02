package io.arenadata.dtm.query.execution.core.base.service.hsql;

import io.vertx.core.Future;
import io.vertx.ext.sql.ResultSet;

import java.util.List;

public interface HSQLClient {

    Future<Void> executeQuery(String query);

    Future<Void> executeBatch(List<String> queries);

    Future<ResultSet> getQueryResult(String query);
}
