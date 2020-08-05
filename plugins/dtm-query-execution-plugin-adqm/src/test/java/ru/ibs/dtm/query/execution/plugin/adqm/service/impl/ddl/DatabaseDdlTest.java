package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.calcite.core.extension.eddl.DropDatabase;
import ru.ibs.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseDdlTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());

    @BeforeAll
    public static void setup() {
        ddlProperties.setCluster("test_cluster");
    }

    @Test
    public void testCreateDatabase() {
        // Create database if not exists

        SqlParserPos pos = new SqlParserPos(1, 1);
        SqlCreateDatabase createDatabase = new SqlCreateDatabase(pos, true,
                new SqlIdentifier("testdb", pos));
        DdlRequestContext context = new DdlRequestContext(null, createDatabase);

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList(t -> t.equalsIgnoreCase("create database if not exists dev__testdb on cluster test_cluster")));

        DropDatabaseExecutor dropDatabaseExecutor = new DropDatabaseExecutor(
                new MockDatabaseExecutor(
                        Collections.singletonList(
                                t -> t.equalsIgnoreCase("drop database if exists dev__testdb on cluster test_cluster"))),
                ddlProperties, appConfiguration);

        DdlExecutor<Void> databaseDdlService = new CreateDatabaseExecutor(executor, ddlProperties, appConfiguration, dropDatabaseExecutor);

        databaseDdlService.execute(context, "CREATE", ar -> assertTrue(ar.succeeded()));

        // Create database
        createDatabase = new SqlCreateDatabase(pos, false,
                new SqlIdentifier("testdb", pos));
        context = new DdlRequestContext(null, createDatabase);

        executor = new MockDatabaseExecutor(
                Collections.singletonList(
                        t -> t.equalsIgnoreCase("create database  dev__testdb on cluster test_cluster")));

        dropDatabaseExecutor = new DropDatabaseExecutor(
                new MockDatabaseExecutor(
                        Collections.singletonList(
                                t -> t.equalsIgnoreCase("drop database if exists dev__testdb on cluster test_cluster"))),
                ddlProperties, appConfiguration);

        databaseDdlService = new CreateDatabaseExecutor(executor, ddlProperties, appConfiguration, dropDatabaseExecutor);

        databaseDdlService.execute(context, "CREATE", ar -> assertTrue(ar.succeeded()));
    }

    @Test
    public void testDropDatabase() {
        SqlParserPos pos = new SqlParserPos(1, 1);
        DropDatabase dropDatabase = new DropDatabase(pos, false,
                new SqlIdentifier("testdb", pos));

        DdlRequestContext context = new DdlRequestContext(null, dropDatabase);

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList(t -> t.equalsIgnoreCase("drop database if exists dev__testdb on cluster test_cluster")));

        DdlExecutor<Void> databaseDdlService = new DropDatabaseExecutor(executor, ddlProperties, appConfiguration);

        databaseDdlService.execute(context, "DROP", ar -> assertTrue(ar.succeeded()));
    }
}