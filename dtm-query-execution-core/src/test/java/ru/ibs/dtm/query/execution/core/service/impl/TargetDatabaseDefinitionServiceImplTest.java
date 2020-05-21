package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;
import org.springframework.plugin.core.SimplePluginRegistry;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.core.service.TargetDatabaseDefinitionService;
import ru.ibs.dtm.query.execution.core.utils.HintExtractor;
import ru.ibs.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TargetDatabaseDefinitionServiceImplTest {

  private TargetDatabaseDefinitionService targetDatabaseDefinitionService =
    new TargetDatabaseDefinitionServiceImpl(
      new DataSourcePluginServiceImpl(
        SimplePluginRegistry.of(
          new DtmDataSourcePlugin() {

            @Override
            public SourceType getSourceType() {
              return SourceType.ADB;
            }

            @Override
            public void ddl(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {

            }

            @Override
            public void llr(LlrRequest request, Handler<AsyncResult<QueryResult>> handler) {
            }

            @Override
            public void mpprKafka(MpprKafkaRequest request, Handler<AsyncResult<QueryResult>> handler) {
            }

            @Override
            public void calcQueryCost(CalcQueryCostRequest request, Handler<AsyncResult<Integer>> handler) {
              handler.handle(Future.succeededFuture(0));
            }
          }
        )
      ), new HintExtractor());

  @Test
  void getTargetSourceOk() {
    Promise promise = Promise.promise();
    QueryRequest request = new QueryRequest();
    request.setSql("select * from dual");

    targetDatabaseDefinitionService.getTargetSource(request, handler -> {
      if (handler.succeeded()) {
        promise.complete(handler.result().getSourceType());
      } else {
        promise.fail(handler.cause());
      }
    });

    assertEquals(SourceType.ADB, promise.future().result());
  }

  @Test
  void getTargetSourceWhenHintExist() {
    Promise promise = Promise.promise();
    QueryRequest request = new QueryRequest();
    request.setSql("select * from dual DATASOURCE_TYPE = ADG");

    targetDatabaseDefinitionService.getTargetSource(request, handler -> {
      if (handler.succeeded()) {
        promise.complete(handler.result().getSourceType());
      } else {
        promise.fail(handler.cause());
      }
    });

    assertEquals(SourceType.ADG, promise.future().result());
  }

  @Test
  void getTargetSourceInformationSchema() {
    QueryRequest request = new QueryRequest();
    request.setSql("select * from information_schema.schemata");

    targetDatabaseDefinitionService.getTargetSource(request, handler -> {
      assertTrue(handler.succeeded());
      assertEquals(SourceType.INFORMATION_SCHEMA, handler.result().getSourceType());
    });
  }

  @Test
  void getTargetSourceInformationSchemaWithQuotes() {
    QueryRequest request = new QueryRequest();
    request.setSql("select * from \"information_schema\".\"schemata\"");

    targetDatabaseDefinitionService.getTargetSource(request, handler -> {
      assertTrue(handler.succeeded());
      assertEquals(SourceType.INFORMATION_SCHEMA, handler.result().getSourceType());
    });
  }
}
