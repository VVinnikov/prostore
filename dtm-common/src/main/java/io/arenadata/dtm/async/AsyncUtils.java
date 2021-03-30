package io.arenadata.dtm.async;


import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public class AsyncUtils {
    public static <T, R> Handler<AsyncResult<T>> succeed(Handler<AsyncResult<R>> handler, Consumer<T> succeed) {
        return ar -> {
            if (ar.succeeded()) {
                try {
                    succeed.accept(ar.result());
                } catch (Exception ex) {
                    log.error("Error: ", ex);
                    handler.handle(Future.failedFuture(ex));
                }
            } else {
                log.error("Error: ", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        };
    }

    public static <T> Future<Void> toEmptyVoidFuture(T any) {
        return Future.succeededFuture();
    }

    public static <T> Future<T> measureMs(Future<T> future, DurationListener durationListener) {
        return Future.future((Promise<Long> p) -> p.complete(System.currentTimeMillis()))
                .compose(startTime -> future.onSuccess(v -> durationListener.onDuration(System.currentTimeMillis() - startTime)));
    }
}
