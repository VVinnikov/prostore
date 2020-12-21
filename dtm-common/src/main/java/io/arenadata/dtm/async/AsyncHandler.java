package io.arenadata.dtm.async;

import io.arenadata.dtm.common.exception.DtmException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public interface AsyncHandler<T> extends Handler<AsyncResult<T>> {

    default void handleSuccess(T result) {
        handle(Future.succeededFuture(result));
    }

    default void handleSuccess() {
        handle(Future.succeededFuture());
    }

    default void handleError(String errMsg, Throwable error) {
        handle(Future.failedFuture(new DtmException(errMsg, error)));
    }

    default void handleError(String errMsg) {
        handle(Future.failedFuture(new DtmException(errMsg)));
    }

    default void handleError(Throwable error) {
        handle(Future.failedFuture(error));
    }
}
