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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;

import static io.arenadata.dtm.query.execution.core.integration.util.FileUtil.getFileContent;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@ExtendWith(VertxExtension.class)
public class DdlCoreIT extends AbstractCoreDtmIntegrationTest {

    private static final String EMPTY = "";
    @Autowired
    @Qualifier("testQueryExecutor")
    private QueryExecutor queryExecutor;
    @Autowired
    @Qualifier("coreDtm")
    private GenericContainer<?> dtmCoreContainer;
    @Autowired
    @Qualifier("adqm")
    private GenericContainer<?> adqmContainer;

    @SneakyThrows
    @Test
    void createTableTest() throws IOException {
        TestSuite suite = TestSuite.create("llrInfoSchemaIT");
        Promise<ResultSet> promise = Promise.promise();
        final String query = getFileContent("it/queries/create_table_test_script.sql");
        suite.test("subscrExecuteSuccess", testContext1 -> {
            Async async = testContext1.async();
            queryExecutor.executeQuery(EMPTY, query)
                    .onComplete(result -> {
                        promise.complete(result.result());
                        async.complete();
                    });
            async.awaitSuccess();
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNotNull(promise.future().result());
    }
}
