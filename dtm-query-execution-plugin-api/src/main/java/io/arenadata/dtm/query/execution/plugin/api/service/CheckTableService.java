package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface CheckTableService extends DatamartExecutionService<CheckContext, AsyncResult<Void>> {
    void check(CheckContext context, Handler<AsyncResult<Void>> handler);
}
