package io.arenadata.dtm.query.execution.core.verticle;

import io.arenadata.dtm.async.AsyncHandler;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;

public interface TaskVerticleExecutor {
    <T> void execute(Handler<Promise<T>> codeHandler, AsyncHandler<T> resultHandler);
}
