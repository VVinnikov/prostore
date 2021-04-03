package io.arenadata.dtm.query.execution.plugin.adqm.ddl;

import io.arenadata.dtm.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.service.CreateDatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.ddl.service.DropDatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseDdlTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final String ENV = "dev";
    private static final String DATAMART = "testdb";
    private static final String CLUSTER = "test_cluster";

    @BeforeAll
    public static void setup() {
        ddlProperties.setCluster(CLUSTER);
    }

    @Test
    public void testCreateDatabase() {
        String createTemplate = "CREATE DATABASE IF NOT EXISTS %s__%s ON CLUSTER %s";
        DdlRequest request = DdlRequest.builder()
                .envName(ENV)
                .datamartMnemonic(DATAMART)
                .build();

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList(t -> t.equalsIgnoreCase(String.format(createTemplate, ENV, DATAMART, CLUSTER))));

        DdlExecutor<Void> databaseDdlService = new CreateDatabaseExecutor(executor, ddlProperties);

        databaseDdlService.execute(request).onComplete(ar -> assertTrue(ar.succeeded()));
    }

    @Test
    public void testDropDatabase() {
        String dropTemplate = "DROP DATABASE IF EXISTS %s__%s ON CLUSTER %s";
        DdlRequest request = DdlRequest.builder()
                .envName(ENV)
                .datamartMnemonic(DATAMART)
                .build();

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList(t -> t.equalsIgnoreCase(String.format(dropTemplate, ENV, DATAMART, CLUSTER))));

        DdlExecutor<Void> databaseDdlService = new DropDatabaseExecutor(executor, ddlProperties);

        databaseDdlService.execute(request).onComplete(ar -> assertTrue(ar.succeeded()));
    }
}
