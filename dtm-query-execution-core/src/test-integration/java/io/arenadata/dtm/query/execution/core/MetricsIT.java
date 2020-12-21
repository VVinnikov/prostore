package io.arenadata.dtm.query.execution.core;

import io.arenadata.dtm.query.execution.core.dto.metrics.ResultMetrics;
import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@ExtendWith(VertxExtension.class)
public class MetricsIT extends AbstractCoreDtmIT {

    @Autowired
    @Qualifier("itTestWebClient")
    private WebClient webClient;

    @Test
    void metricsTest() throws IOException {
        TestSuite suite = TestSuite.create("get metrics tests");
        Promise<ResultMetrics> promise = Promise.promise();
        suite.test("get metrics", context -> {
            Async async = context.async();
            webClient.get(getDtmMetricsPortExternal(), getDtmCoreHostExternal(), "/actuator/requests/")
                    .send(ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result().bodyAsJson(ResultMetrics.class));
                        } else {
                            promise.fail(ar.cause());
                        }
                        async.complete();
                    });
            async.awaitSuccess(5000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
        assertTrue(promise.future().succeeded());
        assertNotNull(promise.future().result());
    }
}
