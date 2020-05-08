package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;

/**
 * Сервис определения стоимости запроса
 */
public interface QueryCostService {

  void calc(CalcQueryCostRequest request, Handler<AsyncResult<Integer>> handler);
}
