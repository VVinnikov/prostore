package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adg.model.QueryResultItem;
import io.arenadata.dtm.query.execution.plugin.adg.service.DtmTestConfiguration;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.dml.AdgLlrService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = DtmTestConfiguration.class)
@ExtendWith(VertxExtension.class)
@EnabledIfEnvironmentVariable(named = "skipITs", matches = "false")
class AdgLlrServiceTest {

    private final QueryEnrichmentService enrichmentService = mock(QueryEnrichmentService.class);
    private final QueryExecutorService executorService = mock(QueryExecutorService.class);
    private final LlrService<QueryResult> llrService = new AdgLlrService(enrichmentService, executorService);

    @Test
    @Disabled("FIXME")
    void testExecuteNotEmptyOk() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("select name from s");

        QueryResultItem queryResultItem = new QueryResultItem(
                Collections.singletonList(new ColumnMetadata("name", ColumnType.VARCHAR)),
                Collections.singletonList(Collections.singletonList("val")));
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put("name", "val");
        result.add(rowMap);
        QueryResult expectedResult = new QueryResult(
                queryRequest.getRequestId(),
                result);

        prepare(queryRequest, expectedResult);

        llrService.execute(new LlrRequestContext(new RequestMetrics(), new LlrRequest(queryRequest, new ArrayList<>(),
                Collections.singletonList(new ColumnMetadata("name", ColumnType.VARCHAR)))))
        .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(expectedResult, promise.future().result());
    }

    @Test
    @Disabled("FIXME")
    void testExecuteEmptyOk() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("select name from s");

        QueryResultItem queryResultItem = new QueryResultItem(
                Collections.singletonList(new ColumnMetadata("name", ColumnType.VARCHAR)),
                Collections.emptyList());
        QueryResult expectedResult = new QueryResult(
                queryRequest.getRequestId(),
                new ArrayList<>());

        prepare(queryRequest, expectedResult);

        llrService.execute(new LlrRequestContext(new RequestMetrics(), new LlrRequest(queryRequest, new ArrayList<>(),
                Collections.singletonList(new ColumnMetadata("name", ColumnType.VARCHAR)))))
        .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(expectedResult, promise.future().result());
    }

    private void prepare(QueryRequest queryRequest, QueryResult expectedResult) {
        when(executorService.execute(any(), any())).thenReturn(Future.succeededFuture(expectedResult.getResult()));
        when(enrichmentService.enrich(any())).thenReturn(Future.succeededFuture(queryRequest.getSql()));
    }
}
