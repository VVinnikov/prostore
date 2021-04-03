package io.arenadata.dtm.query.execution.plugin.adqm.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateDatabaseExecutor implements DdlExecutor<Void> {
    private static final String CREATE_TEMPLATE = "CREATE DATABASE IF NOT EXISTS %s__%s ON CLUSTER %s";

    private final DatabaseExecutor databaseExecutor;
    private final DdlProperties ddlProperties;

    public CreateDatabaseExecutor(DatabaseExecutor databaseExecutor,
                                  DdlProperties ddlProperties) {
        this.databaseExecutor = databaseExecutor;
        this.ddlProperties = ddlProperties;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return Future.future(promise -> {
            createDatabase(request.getEnvName(), request.getDatamartMnemonic())
                    .onComplete(promise);
        });
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

    private Future<Void> createDatabase(String envName, String dbname) {
        String cluster = ddlProperties.getCluster();
        String createCmd = String.format(CREATE_TEMPLATE, envName, dbname, cluster);
        return databaseExecutor.executeUpdate(createCmd);
    }
}
