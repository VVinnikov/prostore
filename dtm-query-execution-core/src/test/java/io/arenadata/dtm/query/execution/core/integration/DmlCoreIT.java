package io.arenadata.dtm.query.execution.core.integration;

import io.arenadata.dtm.query.execution.core.integration.query.executor.QueryExecutor;
import io.vertx.core.Promise;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.junit5.VertxExtension;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@ExtendWith(VertxExtension.class)
public class DmlCoreIT extends AbstractCoreDtmIntegrationTest {

    private static final String EMPTY = "";
    private ResultSet resultSet;
    @Autowired
    @Qualifier("testQueryExecutor")
    private QueryExecutor queryExecutor;

    @SneakyThrows
    @Test
    void InformationSchemaTest() throws IOException {
        TestSuite suite = TestSuite.create("llrInfoSchemaIT");
        suite.test("subscrExecuteSuccess", testContext1 -> {
            Promise<?> promise = Promise.promise();
            Async async = testContext1.async();
            queryExecutor.executeQuery(EMPTY, "select * from information_schema.tables")
                    .onSuccess(result -> {
                        resultSet = result;
                        async.complete();
                    })
                    .onFailure(promise::fail);
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(resultSet);
    }

    @Test
    @Disabled
    void llrAdqmTest() {
        TestSuite suite = TestSuite.create("llrAdqmIT");
        suite.test("subscrExecuteSuccess", testContext1 -> {
            Promise<?> promise = Promise.promise();
            Async async = testContext1.async();

            queryExecutor.executeQuery(EMPTY, "select * from transactions DATASOURCE_TYPE='ADQM'")
                    .onSuccess(result -> {
                        resultSet = result;
                        async.complete();
                    })
                    .onFailure(promise::fail);
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(resultSet);
    }
}
