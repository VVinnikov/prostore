package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import io.arenadata.dtm.query.execution.core.service.dml.impl.InformationSchemaExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.service.hsql.impl.HSQLClientImpl;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.apache.calcite.sql.SqlDialect;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;


public class InformationSchemaExecutorImplTest {

    private HSQLClient client = mock(HSQLClientImpl.class);
    private InformationSchemaExecutor informationSchemaExecutor = new InformationSchemaExecutorImpl(client,
        new SqlDialect(SqlDialect.EMPTY_CONTEXT),
        mock(QueryParserService.class));

    @Test
    @Disabled
    void executeQuery() {
        //FIXME
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

        informationSchemaExecutor.execute(sourceRequest, ar -> {
            assertTrue(ar.succeeded());
            Map<String, Object> expectedMap = new HashMap<>();
            expectedMap.put("schema_name", "test_datamart");
            assertEquals(expectedMap, ar.result().getResult().get(0));
        });
    }
}
