package io.arenadata.dtm.query.execution.core.integration;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.integration.generator.VendorEmulatorService;
import io.arenadata.dtm.query.execution.core.integration.query.executor.QueryExecutor;
import io.arenadata.dtm.query.execution.core.integration.util.FileUtil;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.UUID;

import static io.arenadata.dtm.query.execution.core.integration.util.QueryUtil.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class EdmlIntegrationTest extends AbstractCoreDtmIntegrationTest {

    @Autowired
    @Qualifier("itTestQueryExecutor")
    private QueryExecutor queryExecutor;
    @Autowired
    @Qualifier("itTestWebClient")
    private WebClient webClient;
    @Autowired
    private VendorEmulatorService vendorEmulator;
    @Autowired
    @Qualifier("itTestZkKafkaProvider")
    private KafkaZookeeperConnectionProvider zookeeperConnectionProvider;

    @Test
    void statusMonitorTest() {
        TestSuite suite = TestSuite.create("status monitor tests");
        Promise<StatusResponse> promise = Promise.promise();
        suite.test("get offset status", context -> {
            Async async = context.async();
            StatusRequest statusRequest = new StatusRequest("test", "");
            webClient.post(getKafkaStatusMonitorPort(), getKafkaStatusMonitorHost(), "/status")
                    .sendJson(statusRequest, ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result().bodyAsJson(StatusResponse.class));
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess(5000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNull(promise.future().cause());
        assertNotNull(promise.future().result());
    }

    @Test
    void vendorEmulatorTest() {
        TestSuite suite = TestSuite.create("vendor emulator tests");
        Promise<?> promise = Promise.promise();
        suite.test("generate data and send into test_topic", context -> {
            Async async = context.async();
            final Object loadRequest = Json.decodeValue(FileUtil.getFileContent("it/requests/generated_data_check_request.json"));
            vendorEmulator.generateData(getVendorEmulatorHost(), getVendorEmulatorPort(), loadRequest)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess(5000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertNull(promise.future().cause());
    }

    @Test
    void mppwTest() {
        TestSuite suite = TestSuite.create("mppw tests");
        Promise<?> promise = Promise.promise();
        final String datamart = "mppw_test";
        final String destinationTable = "test_table";
        final String sourceTable = "test_table_ext";
        final int rowCount = 1000;
        final String topic = destinationTable + UUID.randomUUID();
        suite.test("mppw", context -> {
            Async async = context.async();
            queryExecutor.executeQuery(String.format(CREATE_DB, datamart))
                    .compose(v -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/mppw/create_destination_table.sql"),
                                    datamart,
                                    destinationTable,
                                    SourceType.ADQM.name())))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet.getResults(), "destination table created successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/mppw/create_source_ext_table.sql"),
                                    datamart,
                                    sourceTable,
                                    getZkKafkaConnectionString(),
                                    topic)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "source table created successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> vendorEmulator.generateData(getVendorEmulatorHost(),
                            getVendorEmulatorPort(),
                            Json.decodeValue(String.format(FileUtil.getFileContent("it/requests/generated_data_request.json"),
                                    topic,
                                    rowCount + 1
                            ))))
                    .map(v -> {
                        assertTrue(true, "messages sent to topic successfully");
                        return v;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/delta/begin_delta.sql"),
                                    datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet.getResults().get(0).getString(0),
                                "delta opened successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(INSERT_QUERY,
                            datamart,
                            destinationTable,
                            "*",
                            datamart,
                            sourceTable)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "data inserted successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format(FileUtil.getFileContent("it/queries/delta/commit_delta.sql"),
                                    datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "delta committed successfully");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(
                            String.format("select count(*) as cnt from %s.%s DATASOURCE_TYPE='ADQM'",
                            datamart,
                            destinationTable
                    )))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet.getResults().get(0).getLong(0), "inserted rows count");
                        return resultSet;
                    })
                    .compose(resultSet -> queryExecutor.executeQuery(String.format(DROP_DB, datamart)))
                    .map(resultSet -> {
                        Assertions.assertNotNull(resultSet, "database dropped successfully");
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