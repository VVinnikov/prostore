package io.arenadata.dtm.query.execution.core.service.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface LogicViewReplacer {
    void replace(String sql, String datamart, Handler<AsyncResult<String>> resultHandler);
}
