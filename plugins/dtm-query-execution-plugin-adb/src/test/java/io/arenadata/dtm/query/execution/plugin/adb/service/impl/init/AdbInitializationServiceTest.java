package io.arenadata.dtm.query.execution.plugin.adb.service.impl.init;

import io.arenadata.dtm.query.execution.plugin.adb.factory.AdbHashFunctionFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.AdbHashFunctionFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdbInitializationServiceTest {

    private final AdbQueryExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private final AdbHashFunctionFactory hashFunctionFactory = new AdbHashFunctionFactoryImpl();
    private final AdbInitializationService initializationService = new AdbInitializationService(adbQueryExecutor, hashFunctionFactory);

    @Test
    void executeSuccess() {
        when(adbQueryExecutor.executeUpdate(any()))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(adbQueryExecutor, times(1)).executeUpdate(any());
                });

    }

    @Test
    void executeQueryError() {
        when(adbQueryExecutor.executeUpdate(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("")));

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(adbQueryExecutor, times(1)).executeUpdate(any());
                });
    }
}