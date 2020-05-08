package ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.cost;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

@Service("adbQueryCostService")
public class AdbQueryCostService implements QueryCostService {

  @Override
  public void calc(CalcQueryCostRequest request, Handler<AsyncResult<Integer>> handler) {
    handler.handle(Future.succeededFuture(0));
  }
}
