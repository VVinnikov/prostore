package io.arenadata.dtm.query.execution.plugin.adb.service.impl.dml;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.cache.service.CaffeineCacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.dialect.LimitSqlDialect;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.dml.service.AdbLlrService;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.query.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.enrichment.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
class AdbLlrServiceTest {
    private final static String template = "SELECT * from PSO";
    private LlrService<QueryResult> adbLLRService;

    @BeforeEach
    void init() {
        AsyncResult<Void> asyncResultEmpty = mock(AsyncResult.class);
        when(asyncResultEmpty.succeeded()).thenReturn(true);

        AsyncResult<List<List<?>>> asyncResult = mock(AsyncResult.class);
        when(asyncResult.succeeded()).thenReturn(true);
        when(asyncResult.result()).thenReturn(new ArrayList<>());
        QueryEnrichmentService adbQueryEnrichmentService = mock(QueryEnrichmentService.class);
        when(adbQueryEnrichmentService.enrich(any()))
                .thenReturn(Future.succeededFuture(template));
        DatabaseExecutor adbDatabaseExecutor = mock(DatabaseExecutor.class);
        when(adbDatabaseExecutor.execute(any(), any()))
                .thenReturn(Future.succeededFuture(new ArrayList<>()));
        when(adbDatabaseExecutor.executeWithParams(any(), any(), any()))
                .thenReturn(Future.succeededFuture(new ArrayList<>()));
        QueryTemplateResult queryTemplateResult = mock(QueryTemplateResult.class);
        when(queryTemplateResult.getTemplate()).thenReturn(template);
        SqlCharStringLiteral sqlNode = SqlLiteral.createCharString("", SqlParserPos.ZERO);
        when(queryTemplateResult.getTemplateNode()).thenReturn(sqlNode);
        QueryTemplateExtractor queryTemplateExtractor = mock(AbstractQueryTemplateExtractor.class);
        when(queryTemplateExtractor.extract(anyString(), any())).thenReturn(queryTemplateResult);
        when(queryTemplateExtractor.extract(any(SqlNode.class), any())).thenReturn(queryTemplateResult);
        when(queryTemplateExtractor.enrichTemplate(any())).thenReturn(sqlNode);
        CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService = mock(CaffeineCacheService.class);
        when(queryCacheService.put(any(), any())).thenReturn(Future.succeededFuture());
        adbLLRService = new AdbLlrService(adbQueryEnrichmentService,
                adbDatabaseExecutor,
                queryCacheService,
                queryTemplateExtractor,
                new LimitSqlDialect(SqlDialect.EMPTY_CONTEXT
                        .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
                        .withIdentifierQuoteString("")
                        .withUnquotedCasing(Casing.TO_LOWER)
                        .withCaseSensitive(false)
                        .withQuotedCasing(Casing.UNCHANGED)));
    }

    @Test
    void executeQuery() {
        List<Datamart> schema = Collections.singletonList(
                new Datamart("TEST_DATAMART", false, Collections.emptyList()));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(template);
        UUID uuid = UUID.randomUUID();
        queryRequest.setRequestId(uuid);
        queryRequest.setDatamartMnemonic("TEST_DATAMART");
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(template);
        QueryTemplateResult queryTemplateResult = new QueryTemplateResult(template, sqlNode, Collections.emptyList());
        LlrRequest llrRequest = LlrRequest.builder()
                .sourceQueryTemplateResult(queryTemplateResult)
                .withoutViewsQuery(sqlNode)
                .originalQuery(sqlNode)
                .requestId(uuid)
                .envName("test")
                .metadata(Collections.emptyList())
                .schema(schema)
                .deltaInformations(Collections.emptyList())
                .datamartMnemonic("TEST_DATAMART")
                .build();
        adbLLRService.execute(llrRequest)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    QueryResult result = ar.result();
                    assertEquals(uuid, result.getRequestId());
                });
    }
}
