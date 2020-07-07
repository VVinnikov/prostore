package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.AbstractEnvironment;
import ru.ibs.dtm.common.calcite.eddl.DropDatabase;
import ru.ibs.dtm.common.calcite.eddl.SqlCreateDatabase;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseDdlServiceTest {
    private static class MockEnvironment extends AbstractEnvironment {
        @Override
        public <T> T getProperty(String key, Class<T> targetType) {
            if (key.equals("env.name")) {
                return (T) "dev";
            }

            if (key.equals("env.defaultDatamart")) {
                return (T) "test_datamart";
            }

            return super.getProperty(key, targetType);
        }
    }

    private static class MockDatabaseExecutor implements DatabaseExecutor {
        private final List<String> expectedCalls;
        private int callCount;

        private MockDatabaseExecutor(List<String> expectedCalls) {
            this.expectedCalls = expectedCalls;
        }

        @Override
        public void execute(String sql, Handler<AsyncResult<JsonArray>> resultHandler) {
            callCount++;

            if (expectedCalls.size() < callCount) {
                resultHandler.handle(Future.failedFuture(
                        String.format("Extra call to DatabaseExecutor, expected %d, got %d", expectedCalls.size(), callCount)));
            }

            String expected = expectedCalls.get(callCount - 1);
            if (!sql.equalsIgnoreCase(expected)) {
                resultHandler.handle(Future.failedFuture(String.format("Incorrect SQL, expected %s, got %s", sql, expected)));
            }

            resultHandler.handle(Future.succeededFuture());
        }

        @Override
        public void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler) {

        }

        @Override
        public void executeWithParams(String sql, List<Object> params, Handler<AsyncResult<?>> resultHandler) {

        }
    }

    public DatabaseDdlService createDdlService(DatabaseExecutor databaseExecutor) {
        ClickhouseProperties clickhouseProperties = new ClickhouseProperties();
        clickhouseProperties.setCluster("test_cluster");

        AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());

        return new DatabaseDdlService(databaseExecutor, clickhouseProperties, appConfiguration);
    }

    @Test
    public void testCreateDatabase() {
        // Create database if not exists

        SqlParserPos pos = new SqlParserPos(1, 1);
        SqlCreateDatabase createDatabase = new SqlCreateDatabase(pos, true,
                new SqlIdentifier("testdb", pos));
        DdlRequestContext context = new DdlRequestContext(null, createDatabase);

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList("create database if not exists dev__testdb on cluster test_cluster"));

        DatabaseDdlService databaseDdlService = createDdlService(executor);

        databaseDdlService.createDatabase(context).onComplete(ar -> assertTrue(ar.succeeded()));

        // Create database
        createDatabase = new SqlCreateDatabase(pos, false,
                new SqlIdentifier("testdb", pos));
        context = new DdlRequestContext(null, createDatabase);

        executor = new MockDatabaseExecutor(
                Arrays.asList(
                        "drop database if exists dev__testdb on cluster test_cluster",
                        "create database  dev__testdb on cluster test_cluster"));

        databaseDdlService = createDdlService(executor);

        databaseDdlService.createDatabase(context).onComplete(ar -> assertTrue(ar.succeeded()));
    }

    @Test
    public void testDropDatabase() {
        SqlParserPos pos = new SqlParserPos(1, 1);
        DropDatabase dropDatabase = new DropDatabase(pos, false,
                new SqlIdentifier("testdb", pos));

        DdlRequestContext context = new DdlRequestContext(null, dropDatabase);

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList("drop database if exists dev__testdb on cluster test_cluster"));

        DatabaseDdlService databaseDdlService = createDdlService(executor);

        databaseDdlService.dropDatabase(context).onComplete(ar -> assertTrue(ar.succeeded()));
    }
}