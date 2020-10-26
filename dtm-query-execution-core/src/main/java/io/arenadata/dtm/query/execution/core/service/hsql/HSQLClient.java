package io.arenadata.dtm.query.execution.core.service.hsql;

import io.vertx.core.Future;

import java.util.List;

public interface HSQLClient {

    Future<Void> executeQuery(String query);

    Future<Void> executeBatch(List<String> queries);

}
