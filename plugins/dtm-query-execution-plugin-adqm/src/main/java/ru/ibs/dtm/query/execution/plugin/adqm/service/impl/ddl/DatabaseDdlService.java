package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.calcite.eddl.DropDatabase;
import ru.ibs.dtm.common.calcite.eddl.SqlCreateDatabase;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

@Component
@Slf4j
public class DatabaseDdlService {
    private static final String CREATE_TEMPLATE = "CREATE DATABASE %s %s__%s ON CLUSTER %s";
    private static final String DROP_TEMPLATE = "DROP DATABASE IF EXISTS %s__%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final ClickhouseProperties clickhouseProperties;
    private final AppConfiguration appConfiguration;

    public DatabaseDdlService(final DatabaseExecutor databaseExecutor,
                              final ClickhouseProperties clickhouseProperties,
                              final AppConfiguration appConfiguration) {
        this.databaseExecutor = databaseExecutor;
        this.clickhouseProperties = clickhouseProperties;
        this.appConfiguration = appConfiguration;
    }

    public Future<Void> createDatabase(final DdlRequestContext context) {
        SqlNode query = context.getQuery();
        if (!(query instanceof SqlCreateDatabase)) {
            return Future.failedFuture(String.format("Expecting SqlCreateDatabase in context, receiving: %s", context));
        }

        String name = ((SqlCreateDatabase) query).getName().names.get(0);
        boolean ifNotExists = ((SqlCreateDatabase) query).ifNotExists();

        return ifNotExists ?
                createDatabase(name, true) :
                dropDatabase(name).compose(v -> createDatabase(name, false));
    }

    public Future<Void> dropDatabase(DdlRequestContext context) {
        SqlNode query = context.getQuery();
        if (!(query instanceof DropDatabase)) {
            return Future.failedFuture(String.format("Expecting DropDatabase in context, receiving: %s", context));
        }

        String name = ((DropDatabase) query).getName().names.get(0);

        return dropDatabase(name);
    }

    private Future<Void> createDatabase(String dbname, boolean ifNotExists) {
        String ifNotExistsKeyword = ifNotExists ? "IF NOT EXISTS" : "";
        String cluster = clickhouseProperties.getCluster();

        String createCmd = String.format(CREATE_TEMPLATE, ifNotExistsKeyword, appConfiguration.getSystemName(),
                dbname, cluster);
        return execute(createCmd);
    }

    private Future<Void> dropDatabase(String dbname) {
        String cluster = clickhouseProperties.getCluster();
        String dropCmd = String.format(DROP_TEMPLATE, appConfiguration.getSystemName(), dbname, cluster);
        return execute(dropCmd);
    }

    private Future<Void> execute(String query) {
        Promise<Void> result = Promise.promise();

        databaseExecutor.execute(query, ar -> {
            if (ar.succeeded()) {
                result.complete();
            } else {
                result.fail(ar.cause());
            }
        });

        return result.future();
    }
}
