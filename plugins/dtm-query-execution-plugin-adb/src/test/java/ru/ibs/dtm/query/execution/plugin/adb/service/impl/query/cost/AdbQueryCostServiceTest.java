package ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.QueryCostRequest;
import utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdbQueryCostServiceTest {
    private final QueryEnrichmentService adbQueryEnrichmentService = mock(QueryEnrichmentService.class);
    private final AdbQueryCostService costService = new AdbQueryCostService(adbQueryEnrichmentService);

    @Test
    void calc() {
        initEnrichmentMocks();
        val context = getQueryCostRequestContext();
        TestSuite suite = TestSuite.create("test_suite");
        suite.test("executeQuery", testContext -> {
            Async async = testContext.async();
            costService.calc(context, ar -> {
                if (ar.succeeded()) {
                    testContext.assertEquals(0, ar.result());
                    async.complete();
                } else {
                    testContext.fail(ar.cause());
                }
            });
            async.awaitSuccess(5000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    @Test
    void calcWithEnrichmentError() {
        initEnrichmentBadMocks();
        val context = getQueryCostRequestContext();
        TestSuite suite = TestSuite.create("test_suite");
        suite.test("executeQuery", testContext -> {
            Async async = testContext.async();
            costService.calc(context, ar -> {
                if (ar.succeeded()) {
                    testContext.fail();
                } else {
                    testContext.asyncAssertFailure();
                    async.complete();
                }
            });
            async.awaitSuccess(5000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private QueryCostRequestContext getQueryCostRequestContext() {
        JsonObject schema = JsonUtils.init("meta_data.json", "TEST_DATAMART");
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("SELECT * from PSO");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic("TEST_DATAMART");
        QueryCostRequest costRequest = new QueryCostRequest(queryRequest, schema);
        return new QueryCostRequestContext(costRequest);
    }

    private void initEnrichmentMocks() {
        AsyncResult<Void> asyncResultEmpty = mock(AsyncResult.class);
        when(asyncResultEmpty.succeeded()).thenReturn(true);

        AsyncResult<List<List<?>>> asyncResult = mock(AsyncResult.class);
        when(asyncResult.succeeded()).thenReturn(true);
        when(asyncResult.result()).thenReturn(new ArrayList<>());
        doAnswer((Answer<AsyncResult<Void>>) arg0 -> {
            ((Handler<AsyncResult<Void>>) arg0.getArgument(1)).handle(asyncResultEmpty);
            return null;
        }).when(adbQueryEnrichmentService).enrich(any(), any());
    }

    private void initEnrichmentBadMocks() {
        doAnswer((Answer<AsyncResult<Void>>) args -> {
            ((Handler<AsyncResult<Void>>) args.getArgument(1)).handle(Future.failedFuture("Enrichment error"));
            return null;
        }).when(adbQueryEnrichmentService).enrich(any(), any());
    }
}
