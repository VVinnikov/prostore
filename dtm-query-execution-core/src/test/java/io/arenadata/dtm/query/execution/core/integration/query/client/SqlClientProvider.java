package io.arenadata.dtm.query.execution.core.integration.query.client;

import io.vertx.ext.sql.SQLClient;

public interface SqlClientProvider {

    SQLClient get(String datamart);
}
