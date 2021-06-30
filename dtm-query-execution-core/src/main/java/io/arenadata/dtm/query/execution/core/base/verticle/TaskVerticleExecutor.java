package io.arenadata.dtm.query.execution.core.base.verticle;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;

public interface TaskVerticleExecutor {
    <T> Future<T> execute(Handler<Promise<T>> codeHandler);
}
