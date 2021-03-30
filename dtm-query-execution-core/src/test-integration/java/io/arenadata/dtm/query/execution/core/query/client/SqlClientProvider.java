package io.arenadata.dtm.query.execution.core.query.client;

import io.vertx.ext.sql.SQLClient;

public interface SqlClientProvider {

    SQLClient get(String datamart);
}
