package io.arenadata.dtm.query.execution.plugin.adp.rollback;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.rollback.service.AdpRollbackService;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AdpRollbackServiceTest {

    private static final String TABLE = "table";
    private static final String DATAMART = "dtm";

    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdpRollbackService adpRollbackService = new AdpRollbackService(databaseExecutor);

    private final RollbackRequest request = RollbackRequest.builder()
            .datamartMnemonic(DATAMART)
            .entity(Entity.builder()
                    .name(TABLE)
                    .build())
            .sysCn(1L)
            .build();

    @Test
    void rollbackSuccess() {
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());

        adpRollbackService.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));
    }

    @Test
    void rollbackFail() {
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.failedFuture("error"));

        adpRollbackService.execute(request)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals("error", ar.cause().getMessage());
                });
    }
}
