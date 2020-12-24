package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.dml.AdgLlrService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AdgLlrServiceTest {

    private final QueryEnrichmentService enrichmentService = mock(QueryEnrichmentService.class);
    private final QueryExecutorService executorService = mock(QueryExecutorService.class);
    private final LlrService<QueryResult> llrService = new AdgLlrService(enrichmentService, executorService);

    @Test
    void testExecuteNotEmptyOk() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("select name from s");

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
    void testExecuteEmptyOk() {
        Promise<QueryResult> promise = Promise.promise();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("select name from s");
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
