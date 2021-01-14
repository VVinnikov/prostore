package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.val;
import org.junit.jupiter.api.Test;
import utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
            costService.calc(context);
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
            costService.calc(context);
            async.awaitSuccess(5000);
        });
        suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
    }

    private QueryCostRequestContext getQueryCostRequestContext() {
        JsonObject jsonSchema = JsonUtils.init("meta_data.json", "TEST_DATAMART");
        List<Datamart> schema = new ArrayList<>();
        schema.add(jsonSchema.mapTo(Datamart.class));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql("SELECT * from PSO");
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setDatamartMnemonic("TEST_DATAMART");
        QueryCostRequest costRequest = new QueryCostRequest(queryRequest, schema);
        return new QueryCostRequestContext(new RequestMetrics(), costRequest, query);
    }

    private void initEnrichmentMocks() {
        AsyncResult<Void> asyncResultEmpty = mock(AsyncResult.class);
        when(asyncResultEmpty.succeeded()).thenReturn(true);

        AsyncResult<List<List<?>>> asyncResult = mock(AsyncResult.class);
        when(asyncResult.succeeded()).thenReturn(true);
        when(asyncResult.result()).thenReturn(new ArrayList<>());
        when(adbQueryEnrichmentService.enrich(any())).thenReturn(Future.succeededFuture());
    }

    private void initEnrichmentBadMocks() {
        when(adbQueryEnrichmentService.enrich(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("Enrichment error")));
    }
}
