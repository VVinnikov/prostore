package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.cache.service.CaffeineCacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adg.service.DtmTestConfiguration;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.dml.AdgLlrService;
import io.arenadata.dtm.query.execution.plugin.adg.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.junit5.VertxExtension;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = DtmTestConfiguration.class)
@ExtendWith(VertxExtension.class)
@EnabledIfEnvironmentVariable(named = "skipITs", matches = "false")
class AdgLlrServiceTest {

    private final QueryEnrichmentService enrichmentService = mock(QueryEnrichmentService.class);
    private final QueryExecutorService executorService = mock(QueryExecutorService.class);
    private final CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService = mock(CaffeineCacheService.class);
    private final QueryTemplateExtractor queryTemplateExtractor = mock(AbstractQueryTemplateExtractor.class);
    private final LlrService<QueryResult> llrService = new AdgLlrService(enrichmentService,
            executorService,
            queryCacheService,
            queryTemplateExtractor,
            new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT
                    .withDatabaseProduct(SqlDialect.DatabaseProduct.UNKNOWN)
                    .withIdentifierQuoteString("\"")
                    .withUnquotedCasing(Casing.TO_LOWER)
                    .withCaseSensitive(false)
                    .withQuotedCasing(Casing.UNCHANGED)),
            new AdgTemplateParameterConverter());

    @Test
    void testExecuteNotEmptyOk() {
        String sql = "select name from s";
        UUID requestId = UUID.randomUUID();
        Promise<QueryResult> promise = Promise.promise();
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put("name", "val");
        result.add(rowMap);
        QueryResult expectedResult = new QueryResult(
                requestId,
                result);
        prepare(sql, expectedResult);
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        LlrRequest llrRequest = LlrRequest.builder()
                .metadata(Collections.singletonList(new ColumnMetadata("name", ColumnType.VARCHAR)))
                .sourceQueryTemplateResult(new QueryTemplateResult(sql, sqlNode, Collections.emptyList()))
                .schema(new ArrayList<>())
                .requestId(requestId)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .build();

        llrService.execute(llrRequest)
                .onComplete(promise);


        assertTrue(promise.future().succeeded());
        assertEquals(expectedResult, promise.future().result());
    }

    @Test
    void testExecuteEmptyOk() {
        String sql = "select name from s";
        UUID requestId = UUID.randomUUID();
        Promise<QueryResult> promise = Promise.promise();
        QueryResult expectedResult = new QueryResult(
                requestId,
                new ArrayList<>());
        prepare(sql, expectedResult);
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        LlrRequest llrRequest = LlrRequest.builder()
                .metadata(Collections.singletonList(new ColumnMetadata("name", ColumnType.VARCHAR)))
                .sourceQueryTemplateResult(new QueryTemplateResult(sql, sqlNode, Collections.emptyList()))
                .requestId(requestId)
                .schema(new ArrayList<>())
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .build();

        llrService.execute(llrRequest)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
        assertEquals(expectedResult, promise.future().result());
    }

    private void prepare(String sql, QueryResult expectedResult) {
        when(executorService.execute(any(), null, any())).thenReturn(Future.succeededFuture(expectedResult.getResult()));
        when(enrichmentService.enrich(any())).thenReturn(Future.succeededFuture(sql));
    }
}
