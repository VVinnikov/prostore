package io.arenadata.dtm.query.execution.core.base.verticle.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.VertxPoolProperties;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
class TaskVerticleExecutorImplTest {
    private final VertxPoolProperties vertxPoolProperties = new VertxPoolProperties();
    private TaskVerticleExecutorImpl taskVerticleExecutor;

    @BeforeEach
    void setUp(Vertx vertx) throws InterruptedException {
        vertxPoolProperties.setTaskTimeout(100L);
        taskVerticleExecutor = new TaskVerticleExecutorImpl(vertxPoolProperties);
        vertx.deployVerticle(taskVerticleExecutor);
        Thread.sleep(100);
    }

    @Test
    void shouldSuccessWhenFutureSucceed(VertxTestContext testContext) {
        // act
        String result = "RESULT";
        taskVerticleExecutor.execute((Handler<Promise<String>>) event -> Future.succeededFuture(result).onComplete(event))
                .onComplete(event -> {
                    testContext.verify(() -> {
                        // assert
                        if (event.failed()) {
                            fail(event.cause());
                        }
                        assertEquals(result, event.result());
                    });
                    testContext.completeNow();
                });
    }

    @Test
    void shouldSuccessWhenVoidFutureSucceed(VertxTestContext testContext) {
        // act
        taskVerticleExecutor.execute(event -> Future.succeededFuture().onComplete(event))
                .onComplete(event -> {
                    testContext.verify(() -> {
                        // assert
                        if (event.failed()) {
                            fail(event.cause());
                        }
                        assertNull(event.result());
                    });
                    testContext.completeNow();
                });
    }

    @Test
    void shouldTimeoutWhenExecuteTimeout(VertxTestContext testContext) {
        // arrange
        Handler<Promise<Object>> handler = promise -> Future.future(p -> {
            try {
                Thread.sleep(150L);
                p.complete();
            } catch (InterruptedException e) {
                p.fail(e);
            }
        }).onComplete(promise);

        // act
        taskVerticleExecutor.execute(handler)
                .onComplete(event -> {
                    testContext.verify(() -> {
                        if (event.succeeded()) {
                            fail("Unexpected success");
                        }
                        assertSame(ReplyException.class, event.cause().getClass());
                        assertTrue(event.cause().getMessage().contains("Timed out"));
                    });
                    testContext.completeNow();
                });
    }

    @Test
    void shouldErrorWhenFutureEndedUpWithError(VertxTestContext testContext) {
        // act
        DtmException exception = new DtmException("Exception");
        taskVerticleExecutor.execute(event -> Future.failedFuture(exception).onComplete(event))
                .onComplete(event -> {
                    testContext.verify(() -> {
                        // assert
                        if (event.succeeded()) {
                            fail("Unexpected success");
                        }
                        assertSame(exception, event.cause());
                    });
                    testContext.completeNow();
                });
    }
}