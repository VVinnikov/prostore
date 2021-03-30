package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.vertx.core.Future;
import lombok.NonNull;

import static java.lang.String.format;

public abstract class AbstractMppwRequestHandler implements MppwRequestHandler {
    private static final String DROP_TEMPLATE = "DROP TABLE IF EXISTS %s ON CLUSTER %s";

    protected final DatabaseExecutor databaseExecutor;
    protected final DdlProperties ddlProperties;

    protected AbstractMppwRequestHandler(DatabaseExecutor databaseExecutor, DdlProperties ddlProperties) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
    }

    protected Future<Void> dropTable(@NonNull String table) {
        return databaseExecutor.executeUpdate(format(DROP_TEMPLATE, table, ddlProperties.getCluster()));
    }
}
