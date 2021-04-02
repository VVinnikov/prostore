package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl;

import io.arenadata.dtm.query.execution.plugin.adg.base.service.query.QueryExecutorService;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@ExtendWith(VertxExtension.class)
class QueryExecutorServiceImplIT {

    @Autowired
    QueryExecutorService executorService;

    @Test
    void execute(VertxTestContext testContext) throws Throwable {
        executorService.execute("select * FROM DOC limit 1", null, Collections.emptyList())
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        testContext.awaitCompletion(5, TimeUnit.SECONDS);
    }
}
