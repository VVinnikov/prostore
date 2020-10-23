package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.common.reader.EnrichedQueryRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface QueryService {
  void execute(EnrichedQueryRequest request, Handler<AsyncResult<Void>> handler);
}
