package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.ddl.DdlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.ddl.impl.DdlServiceDaoImpl;
import ru.ibs.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import ru.ibs.dtm.query.execution.core.service.dml.impl.InformationSchemaExecutorImpl;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class InformationSchemaExecutorImplTest {

    private ServiceDbFacade serviceDbFacade = mock(ServiceDbFacade.class);
    private DdlServiceDao ddlServiceDao = mock(DdlServiceDaoImpl.class);
    private InformationSchemaExecutor informationSchemaExecutor = new InformationSchemaExecutorImpl(serviceDbFacade);

    @Test
    void executeQuery() {
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

        when(serviceDbFacade.getDdlServiceDao()).thenReturn(ddlServiceDao);

        doAnswer(invocation -> {
            Handler<AsyncResult<List<Map<String, Object>>>> resultHandler = invocation.getArgument(2);
            resultHandler.handle(Future.succeededFuture(Collections.singletonList(rowMap)));
            return null;
        }).when(ddlServiceDao).executeQuery(any(), any(), any());

        informationSchemaExecutor.execute(sourceRequest, ar -> {
            assertTrue(ar.succeeded());
            Map<String, Object> expectedMap = new HashMap<>();
            expectedMap.put("schema_name", "test_datamart");
            assertEquals(expectedMap, ar.result().getResult().get(0));
        });
    }
}
