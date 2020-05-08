package ru.ibs.dtm.query.execution.plugin.adb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.service.DeltaService;

import java.util.Collections;
import java.util.List;

@TestConfiguration
public class DtmTestConfiguration {

  @Bean("adbDeltaService")
  public DeltaService deltaService() {
    return new DeltaService() {
      @Override
      public void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> handler) {
        //TODO заглушка
        handler.handle(Future.succeededFuture(1L));
      }

      @Override
      public void getDeltasOnDateTimes(List<ActualDeltaRequest> list, Handler<AsyncResult<List<Long>>> handler) {
        //TODO заглушка
        handler.handle(Future.succeededFuture(Collections.singletonList(1L)));
      }
    };
  }
}
