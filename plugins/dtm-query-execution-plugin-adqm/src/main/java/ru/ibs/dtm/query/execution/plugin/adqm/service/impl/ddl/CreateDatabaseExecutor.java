package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import ru.ibs.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;

public class CreateDatabaseExecutor implements DdlExecutor<Void> {
    private static final String CREATE_TEMPLATE = "CREATE DATABASE %s %s__%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final ClickhouseProperties clickhouseProperties;
    private final AppConfiguration appConfiguration;

    public CreateDatabaseExecutor(DatabaseExecutor databaseExecutor, ClickhouseProperties clickhouseProperties, AppConfiguration appConfiguration) {
        this.databaseExecutor = databaseExecutor;
        this.clickhouseProperties = clickhouseProperties;
        this.appConfiguration = appConfiguration;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<Void>> handler) {
        SqlNode query = context.getQuery();
        if (!(query instanceof SqlCreateDatabase)) {
            handler.handle(Future.failedFuture(String.format("Expecting SqlCreateDatabase in context, receiving: %s", context)));
            return;
        }

        String name = ((SqlCreateDatabase) query).getName().names.get(0);
        boolean ifNotExists = ((SqlCreateDatabase) query).ifNotExists();

        createDatabase(name, ifNotExists).onComplete(handler);
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }

    private Future<Void> createDatabase(String dbname, boolean ifNotExists) {
        String ifNotExistsKeyword = ifNotExists ? "IF NOT EXISTS" : "";
        String cluster = clickhouseProperties.getCluster();

        String createCmd = String.format(CREATE_TEMPLATE, ifNotExistsKeyword, appConfiguration.getSystemName(),
                dbname, cluster);
        return databaseExecutor.executeUpdate(createCmd);
    }
}
