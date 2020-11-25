package io.arenadata.dtm.query.execution.core.integration.query.client;

import io.vertx.ext.sql.SQLClient;

public interface SqlClientFactory {
    SQLClient create(String datamartMnemonic);
}
