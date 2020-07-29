package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import ru.ibs.dtm.query.execution.core.service.dml.impl.InformationSchemaExecutorImpl;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class InformationSchemaExecutorImplTest {

    private ServiceDbFacade serviceDbFacade = mock(ServiceDbFacade.class);
    private InformationSchemaExecutor informationSchemaExecutor = new InformationSchemaExecutorImpl(serviceDbFacade);

    @Test
    void executeQuery() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("select * from \"INFORMATION_SCHEMA\".schemata");

        doAnswer(invocation -> {
            Handler<AsyncResult<ResultSet>> resultHandler = invocation.getArgument(1);
            resultHandler.handle(Future.succeededFuture(
                    new ResultSet(
                            Collections.singletonList("schema_name"),
                            Collections.singletonList(
                                    new JsonArray(Collections.singletonList("test_datamart"))),
                            null
                    )
            ));
            return null;
        }).when(serviceDbFacade.getDdlServiceDao()).executeQuery(any(), any());

        informationSchemaExecutor.execute(queryRequest, ar -> {
            assertTrue(ar.succeeded());
            assertEquals(new JsonObject().put("schema_name", "test_datamart"),
                    ar.result().getResult().getList().get(0));
        });
    }
}
