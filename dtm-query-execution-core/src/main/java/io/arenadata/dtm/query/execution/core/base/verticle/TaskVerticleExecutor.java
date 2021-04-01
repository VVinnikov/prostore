package io.arenadata.dtm.query.execution.core.base.verticle;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;

public interface TaskVerticleExecutor {
    <T> void execute(Handler<Promise<T>> codeHandler, Handler<AsyncResult<T>> resultHandler);
}
