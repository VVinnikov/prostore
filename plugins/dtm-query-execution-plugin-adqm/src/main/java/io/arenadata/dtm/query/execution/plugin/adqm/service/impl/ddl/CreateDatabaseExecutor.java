package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateDatabaseExecutor implements DdlExecutor<Void> {
    private static final String CREATE_TEMPLATE = "CREATE DATABASE IF NOT EXISTS %s__%s ON CLUSTER %s";

    private final AppConfiguration appConfiguration;
    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;

    public CreateDatabaseExecutor(DatabaseExecutor databaseExecutor,
                                  DdlProperties ddlProperties,
                                  AppConfiguration appConfiguration) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
        this.appConfiguration = appConfiguration;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, AsyncHandler<Void> handler) {
        SqlNode query = context.getQuery();
        if (!(query instanceof SqlCreateDatabase)) {
            handler.handleError(new DdlDatasourceException(
                    String.format("Expecting SqlCreateDatabase in context, receiving: %s",
                    context)));
            return;
        }

        String name = ((SqlCreateDatabase) query).getName().names.get(0);
        createDatabase(name).onComplete(handler);
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_SCHEMA;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adqmDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }

    private Future<Void> createDatabase(String dbname) {
        String cluster = ddlProperties.getCluster();
        String createCmd = String.format(CREATE_TEMPLATE, appConfiguration.getSystemName(), dbname, cluster);
        return databaseExecutor.executeUpdate(createCmd);
    }
}
