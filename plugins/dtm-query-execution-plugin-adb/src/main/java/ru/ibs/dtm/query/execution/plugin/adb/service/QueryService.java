package ru.ibs.dtm.query.execution.plugin.adb.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.EnrichedQueryRequest;

public interface QueryService {
  void execute(EnrichedQueryRequest request, Handler<AsyncResult<Void>> handler);
}
