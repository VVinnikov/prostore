package io.arenadata.dtm.query.execution.core.integration;

import io.arenadata.dtm.query.execution.core.integration.query.executor.QueryExecutor;
import io.arenadata.dtm.query.execution.core.integration.util.FileUtil;
import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.junit5.VertxExtension;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;

import static io.arenadata.dtm.query.execution.core.integration.util.QueryUtil.*;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith(VertxExtension.class)
public class DdlIntegrationTest extends AbstractCoreDtmIntegrationTest {

    @Autowired
    @Qualifier("itTestQueryExecutor")
    private QueryExecutor queryExecutor;

    @SneakyThrows
    @Test
    void dbTest() throws IOException {
        TestSuite suite = TestSuite.create("db tests");
        final String datamart = "test";
        Promise<?> promise = Promise.promise();
        suite.test("create database", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, datamart))
                    .compose(v -> queryExecutor.executeQuery(String.format(SELECT_DATAMART_INFO, datamart.toUpperCase())))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty(), "database created successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_DB, datamart)))
                    .map(resultSet -> {
                        assertNotNull(resultSet, "database dropped successfully");
                        return resultSet;
                    })
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNull(promise.future().cause());
    }

    @Test
    void tableTest() {
        TestSuite suite = TestSuite.create("create table tests");
        Promise<?> promise = Promise.promise();
        final String datamart = "test";
        final String table = "test_table_1";
        suite.test("create table", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, datamart))
                    .compose(v -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/create_table.sql"), datamart, table)))
                    .compose(v -> queryExecutor.executeQuery(String.format(SELECT_TABLE_INFO,
                            datamart.toUpperCase(),
                            table.toUpperCase())))
                    .map(resultSet -> {
                        assertFalse(resultSet.getResults().isEmpty(), "table created successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_TABLE, datamart, table)))
                    .map(resultSet -> {
                        assertNotNull(resultSet, "table dropped successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_DB, datamart)))
                    .map(resultSet -> {
                        assertNotNull(resultSet, "database dropped successfully");
                        return resultSet;
                    })
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNull(promise.future().cause());
    }
}
