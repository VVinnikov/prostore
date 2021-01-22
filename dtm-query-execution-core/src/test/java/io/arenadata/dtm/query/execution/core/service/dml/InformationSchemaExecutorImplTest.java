package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.*;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.service.dml.impl.InformationSchemaExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.service.hsql.impl.HSQLClientImpl;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.sql.ResultSet;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class InformationSchemaExecutorImplTest {

    private final HSQLClient client = mock(HSQLClientImpl.class);
    private InformationSchemaExecutor informationSchemaExecutor;

    @BeforeEach
    void init() {
        QueryParserService parserService = mock(QueryParserService.class);
        SqlString sqlString = mock(SqlString.class);
        when(sqlString.getSql()).thenReturn("");
        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlNode.toSqlString(any(SqlDialect.class))).thenReturn(sqlString);
        QueryParserResponse queryParserResponse = new QueryParserResponse(null, null, null, sqlNode);
        when(parserService.parse(any())).thenReturn(Future.succeededFuture(queryParserResponse));
        ResultSet resultSet = new ResultSet(Collections.emptyList(), Collections.emptyList(), null);
        when(client.getQueryResult(anyString())).thenReturn(Future.succeededFuture(resultSet));
        informationSchemaExecutor = new InformationSchemaExecutorImpl(client,
                new SqlDialect(SqlDialect.EMPTY_CONTEXT), parserService);
    }

    @Test
    void executeQuery() {
        Promise<QueryResult> promise = Promise.promise();
        List<ColumnMetadata> metadata = new ArrayList<>();
        metadata.add(new ColumnMetadata("schema_name", ColumnType.VARCHAR));
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        final Map<String, Object> rowMap = new HashMap<>();
        rowMap.put("schema_name", "test_datamart");
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("select * from \"INFORMATION_SCHEMA\".schemata");

        sourceRequest.setQueryRequest(queryRequest);
        sourceRequest.setMetadata(metadata);
        sourceRequest.setSourceType(SourceType.INFORMATION_SCHEMA);
        sourceRequest.setQueryTemplate(new QueryTemplateResult(null, null, null));
        Entity entity = new Entity();
        entity.setSchema("test_datamart");
        entity.setName("test");
        entity.setFields(Collections.emptyList());
        entity.setEntityType(EntityType.TABLE);
        Datamart datamart = new Datamart("test_datamart", false, Collections.singletonList(entity));
        sourceRequest.setLogicalSchema(Collections.singletonList(datamart));

        informationSchemaExecutor.execute(sourceRequest)
                .onComplete(promise);
        assertTrue(promise.future().succeeded());
    }
}
