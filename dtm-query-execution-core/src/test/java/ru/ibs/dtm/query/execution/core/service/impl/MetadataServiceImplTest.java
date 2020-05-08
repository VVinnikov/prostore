package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.service.MetadataService;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class MetadataServiceImplTest {

  private ServiceDao serviceDao = mock(ServiceDao.class);
  private MetadataService metadataService = new MetadataServiceImpl(serviceDao);

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
    }).when(serviceDao).executeQuery(any(), any());

    metadataService.executeQuery(queryRequest, ar -> {
      assertTrue(ar.succeeded());
      assertEquals(new JsonObject().put("schema_name", "test_datamart"),
        ar.result().getResult().getList().get(0));
    });
  }
}
