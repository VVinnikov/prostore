package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.dml;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class AdqmLlrServiceTest {
    private final static String ENRICHED_QUERY = "enriched query";
    private final QueryEnrichmentService queryEnrichmentService = mock(QueryEnrichmentService.class);
    private final DatabaseExecutor executorService = mock(DatabaseExecutor.class);
    private final CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService = mock(CacheService.class);
    private final QueryTemplateExtractor templateExtractor = mock(QueryTemplateExtractor.class);
    private final SqlDialect sqlDialect = mock(SqlDialect.class);
    private final AdqmLlrService adqmLlrService = new AdqmLlrService(queryEnrichmentService, executorService,
            queryCacheService, templateExtractor, sqlDialect);

    @BeforeEach
    void setUp() {
        when(queryCacheService.get(any())).thenReturn(null);
        when(queryEnrichmentService.enrich(any())).thenReturn(Future.succeededFuture(ENRICHED_QUERY));
        when(templateExtractor.extract(anyString(), any()))
                .thenReturn(new QueryTemplateResult("", null, Collections.emptyList()));
        when(queryCacheService.put(any(), any()))
                .thenReturn(Future.succeededFuture(QueryTemplateValue.builder().build()));
        HashMap<String, Object> result = new HashMap<>();
        result.put("column", "value");
        when(executorService.executeWithParams(any(), any(), any()))
                .thenReturn(Future.succeededFuture(Collections.singletonList(result)));
    }

    @Test
    void testExecuteWithCacheSuccess() {
        when(queryCacheService.get(any())).thenReturn(QueryTemplateValue.builder().build());
        SqlNode sqlNode = mock(SqlNode.class);
        SqlString sqlString = mock(SqlString.class);
        when(sqlString.getSql()).thenReturn(ENRICHED_QUERY);
        when(sqlNode.toSqlString(any(SqlDialect.class))).thenReturn(sqlString);
        when(templateExtractor.enrichTemplate(any())).thenReturn(sqlNode);
        List<ColumnMetadata> metadata = Collections.singletonList(ColumnMetadata.builder().build());
        UUID requestId = UUID.randomUUID();
        LlrRequest request = LlrRequest.builder()
                .requestId(requestId)
                .metadata(metadata)
                .sourceQueryTemplateResult(new QueryTemplateResult("", null, Collections.emptyList()))
                .build();
        adqmLlrService.execute(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals("value", ar.result().getResult().get(0).get("column"));
                    assertEquals(metadata, ar.result().getMetadata());
                    assertEquals(requestId, ar.result().getRequestId());
                    verify(queryCacheService, times(1)).get(any());
                    verify(queryCacheService, never()).put(any(), any());
                    verify(executorService, times(1)).executeWithParams(eq(ENRICHED_QUERY), eq(null), eq(metadata));
                });
    }

    @Test
    void testExecuteWithoutCacheSuccess() {
        List<ColumnMetadata> metadata = Collections.singletonList(ColumnMetadata.builder().build());
        UUID requestId = UUID.randomUUID();
        LlrRequest request = LlrRequest.builder()
                .requestId(requestId)
                .metadata(metadata)
                .sourceQueryTemplateResult(new QueryTemplateResult("", null, Collections.emptyList()))
                .build();
        adqmLlrService.execute(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals("value", ar.result().getResult().get(0).get("column"));
                    assertEquals(metadata, ar.result().getMetadata());
                    assertEquals(requestId, ar.result().getRequestId());
                    verify(queryCacheService, times(1)).get(any());
                    verify(queryCacheService, times(1)).put(any(), any());
                    verify(executorService, times(1)).executeWithParams(eq(ENRICHED_QUERY), eq(null), eq(metadata));
                });
    }

    @Test
    void testEnrichQuerySuccess() {
        LlrRequest request = LlrRequest.builder().build();
        adqmLlrService.enrichQuery(request)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(ENRICHED_QUERY, ar.result());
                    verify(queryEnrichmentService, times(1)).enrich(any());
                });
    }

    @Test
    void testQueryExecuteSuccess() {
        adqmLlrService.queryExecute("", null, Collections.emptyList())
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals("value", ar.result().get(0).get("column"));
                    verify(executorService, times(1)).executeWithParams(any(),
                            eq(null),
                            eq(Collections.emptyList()));
                });
    }
}
