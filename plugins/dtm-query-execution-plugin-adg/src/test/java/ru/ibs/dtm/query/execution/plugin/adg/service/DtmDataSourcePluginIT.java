package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypes;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlQueryType;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = DtmTestConfiguration.class)
@ExtendWith(VertxExtension.class)
public class DtmDataSourcePluginIT {

  @Autowired
  private DdlService ddlService;

  private DtmDataSourcePlugin plugin = new DtmDataSourcePlugin() {

    @Override
    public boolean supports(SourceType sourceType) {
      return false;
    }

    @Override
    public SourceType getSourceType() {
      return SourceType.ADG;
    }

    @Override
    public void ddl(DdlRequestContext ddlRequest, Handler<AsyncResult<Void>> handler) {
      ddlService.execute(ddlRequest, handler);
    }

    @Override
    public void llr(LlrRequest llrRequest, Handler<AsyncResult<QueryResult>> handler) {

    }

    @Override
    public void mpprKafka(MpprKafkaRequest mpprKafkaRequest, Handler<AsyncResult<QueryResult>> handler) {

    }

    @Override
    public void calcQueryCost(CalcQueryCostRequest calcQueryCostRequest, Handler<AsyncResult<Integer>> handler) {

    }
  };

  @Test
  void testDdl(VertxTestContext testContext) throws Throwable {
    DdlRequest dto = new DdlRequest(null, new ClassTable("test.test_", Arrays.asList(
      new ClassField("id", ClassTypes.INT.name(), false, true, null),
      new ClassField("test", ClassTypes.VARCHAR.name(), true, false, null)
    )));
    plugin.ddl(new DdlRequestContext(dto, DdlQueryType.CREATE_TABLE), ar -> {
      if (ar.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(ar.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

}
