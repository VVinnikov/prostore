package io.arenadata.dtm.query.execution.plugin.adb.synchronize.service;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutorDelegate;
import io.arenadata.dtm.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.EnumSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class AdbSynchronizeServiceTest {

    private static final Long SUCCEED_DELTA_NUM = 42L;
    private static final Long FAILED_DELTA_NUM = 43L;

    @Mock
    private SynchronizeDestinationExecutorDelegate executorDelegate;

    private AdbSynchronizeService adbSynchronizeService;

    @BeforeEach
    void setUp(Vertx vertx) {
        adbSynchronizeService = new AdbSynchronizeService(executorDelegate, vertx);
    }

    @Test
    void shouldSuccessWhenAllDestinationsReturnSameDeltaNum(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder()
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM))
                .build();
        SynchronizeRequest request = new SynchronizeRequest(UUID.randomUUID(), "dev", "datamart", entity);

        when(executorDelegate.execute(Mockito.any(), Mockito.same(request))).thenReturn(Future.succeededFuture(SUCCEED_DELTA_NUM));

        // act
        Future<Long> future = adbSynchronizeService.execute(request);

        // assert
        future.onComplete(result -> {
            if (!result.succeeded()) {
                ctx.failNow(result.cause());
                return;
            }

            ctx.verify(() -> {
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADB), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADG), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADQM), Mockito.same(request));

                assertEquals(SUCCEED_DELTA_NUM, result.result());
            });
            ctx.completeNow();
        });
    }

    @Test
    void shouldFailWhenOneDestinationReturnDifferentDeltaNum(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder()
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM))
                .build();
        SynchronizeRequest request = new SynchronizeRequest(UUID.randomUUID(), "dev", "datamart", entity);

        when(executorDelegate.execute(Mockito.any(), Mockito.same(request))).thenReturn(Future.succeededFuture(SUCCEED_DELTA_NUM));
        when(executorDelegate.execute(Mockito.eq(SourceType.ADB), Mockito.same(request))).thenReturn(Future.succeededFuture(FAILED_DELTA_NUM));

        // act
        Future<Long> future = adbSynchronizeService.execute(request);

        // assert
        future.onComplete(result -> {
            if (result.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success"));
                return;
            }

            ctx.verify(() -> {
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADB), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADG), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADQM), Mockito.same(request));

                assertSame(SynchronizeDatasourceException.class, result.cause().getClass());
                assertTrue(result.cause().getMessage().contains("result deltaNum not equal"));
            });
            ctx.completeNow();
        });
    }

    @Test
    void shouldFailWhenOneDestinationThrowsException(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder()
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM))
                .build();
        SynchronizeRequest request = new SynchronizeRequest(UUID.randomUUID(), "dev", "datamart", entity);

        when(executorDelegate.execute(Mockito.any(), Mockito.same(request))).thenReturn(Future.succeededFuture(SUCCEED_DELTA_NUM));
        when(executorDelegate.execute(Mockito.eq(SourceType.ADB), Mockito.same(request))).thenReturn(Future.failedFuture(new SynchronizeDatasourceException("Not implemented")));

        // act
        Future<Long> future = adbSynchronizeService.execute(request);

        // assert
        future.onComplete(result -> {
            if (result.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success"));
                return;
            }

            ctx.verify(() -> {
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADB), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADG), Mockito.same(request));
                verify(executorDelegate).execute(Mockito.eq(SourceType.ADQM), Mockito.same(request));

                assertSame(SynchronizeDatasourceException.class, result.cause().getClass());
                assertTrue(result.cause().getMessage().contains("Not implemented"));
            });
            ctx.completeNow();
        });
    }

    @Test
    void shouldNotPanicWhenNoDestinations(VertxTestContext ctx) {
        // arrange
        Entity entity = Entity.builder()
                .destination(Collections.emptySet())
                .build();
        SynchronizeRequest request = new SynchronizeRequest(UUID.randomUUID(), "dev", "datamart", entity);


        // act
        Future<Long> future = adbSynchronizeService.execute(request);

        // assert
        future.onComplete(result -> {
            if (result.succeeded()) {
                ctx.failNow(new AssertionError("Unexpected success"));
                return;
            }

            ctx.verify(() -> {
                assertSame(SynchronizeDatasourceException.class, result.cause().getClass());
                assertTrue(result.cause().getMessage().contains("result deltaNum not equal"));
            });
            ctx.completeNow();
        });
    }

}