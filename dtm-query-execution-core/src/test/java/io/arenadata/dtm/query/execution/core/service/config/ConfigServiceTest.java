package io.arenadata.dtm.query.execution.core.service.config;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigType;
import io.arenadata.dtm.query.execution.core.dto.config.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.service.config.impl.ConfigServiceImpl;
import io.arenadata.dtm.query.execution.core.service.config.impl.ConfigStorageAddDdlExecutor;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigServiceTest {
    private final ConfigStorageAddDdlExecutor configStorageAddDdlExecutor = mock(ConfigStorageAddDdlExecutor.class);
    private final ConfigService<QueryResult> configService = new ConfigServiceImpl();

    @BeforeEach
    void init() {
        when(configStorageAddDdlExecutor.getConfigType()).thenCallRealMethod();
        when(configStorageAddDdlExecutor.execute(any())).thenReturn(Future.succeededFuture(QueryResult.emptyResult()));
        configService.addExecutor(configStorageAddDdlExecutor);
    }

    @Test
    void testConfigStorageAdd() {
        SqlConfigCall sqlConfigCall = mock(SqlConfigCall.class);
        when(sqlConfigCall.getSqlConfigType()).thenReturn(SqlConfigType.CONFIG_STORAGE_ADD);
        ConfigRequestContext context = ConfigRequestContext.builder()
                .sqlConfigCall(sqlConfigCall)
                .build();
        configService.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(QueryResult.emptyResult(), ar.result());
                });
    }

    @Test
    void testNotExistExecutor() {
        SqlConfigCall sqlConfigCall = mock(SqlConfigCall.class);
        when(sqlConfigCall.getSqlConfigType()).thenReturn(null);
        ConfigRequestContext context = ConfigRequestContext.builder()
                .sqlConfigCall(sqlConfigCall)
                .build();
        configService.execute(context).onComplete(ar -> {
            assertTrue(ar.failed());
            assertTrue(ar.cause() instanceof DtmException);
        });
    }
}
