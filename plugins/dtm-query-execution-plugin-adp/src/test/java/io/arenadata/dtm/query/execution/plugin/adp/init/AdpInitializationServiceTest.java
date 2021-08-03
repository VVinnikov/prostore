package io.arenadata.dtm.query.execution.plugin.adp.init;

import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.init.service.AdpInitializationService;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdpInitializationServiceTest {

    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdpInitializationService initializationService = new AdpInitializationService(databaseExecutor);

    @Test
    void executeSuccess() {
        when(databaseExecutor.executeUpdate(any()))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(databaseExecutor, times(1)).executeUpdate(any());
                });

    }

    @Test
    void executeQueryError() {
        when(databaseExecutor.executeUpdate(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("")));

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(databaseExecutor, times(1)).executeUpdate(any());
                });
    }
}