package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.Test;

public class VertxTest {
    @Test
    public void testFailedFuture() {
        Promise<Void> ps = Promise.promise();

        Future<Void> fs = ps.future();
        ps.fail("String fail");
        fs.onComplete(ar -> System.out.println(ar.cause().getMessage()));

        Promise<Void> pe = Promise.promise();

        Future<Void> fe = pe.future();
        pe.fail(new RuntimeException("Exception fail"));
        fe.onComplete(ar -> System.out.println(ar.cause().getMessage()));
    }
}
