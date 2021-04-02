package io.arenadata.dtm.query.execution.core.config;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.config.function.SqlConfigStorageAdd;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.config.dto.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.config.service.impl.ConfigStorageAddDdlExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.init.service.CoreInitializationService;
import io.arenadata.dtm.query.execution.core.init.service.impl.CoreInitializationServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.request.ConfigRequest;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ConfigStorageAddDdlExecutorTest {
    private final CalciteDefinitionService calciteDefinitionService = mock(CalciteDefinitionService.class);
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginService.class);
    private final CoreInitializationService initializationService = mock(CoreInitializationServiceImpl.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final ConfigStorageAddDdlExecutor configStorageAddDdlExecutor =
            new ConfigStorageAddDdlExecutor(calciteDefinitionService, dataSourcePluginService, datamartDao, initializationService);

    @BeforeEach
    void init() {
        when(dataSourcePluginService.getSourceTypes()).thenReturn(Stream.of(SourceType.ADB).collect(Collectors.toSet()));
        when(datamartDao.getDatamarts()).thenReturn(Future.succeededFuture(Collections.singletonList("schema")));
        when(dataSourcePluginService.ddl(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(calciteDefinitionService.processingQuery(any())).thenReturn(mock(SqlNode.class));
        when(initializationService.execute(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testExecuteSuccess() {
        SqlConfigStorageAdd configStorageAdd = mock(SqlConfigStorageAdd.class);
        when(configStorageAdd.getSourceType()).thenReturn(SourceType.ADB);
        ConfigRequestContext context = ConfigRequestContext.builder()
                .request(new ConfigRequest(new QueryRequest()))
                .sqlConfigCall(configStorageAdd)
                .build();
        configStorageAddDdlExecutor.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(QueryResult.emptyResult(), ar.result());
                });
    }

    @Test
    void testSourceTypeFail() {
        SqlConfigStorageAdd configStorageAdd = mock(SqlConfigStorageAdd.class);
        when(configStorageAdd.getSourceType()).thenReturn(SourceType.ADG);
        ConfigRequestContext context = ConfigRequestContext.builder()
                .request(new ConfigRequest(new QueryRequest()))
                .sqlConfigCall(configStorageAdd)
                .build();
        configStorageAddDdlExecutor.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                });
    }

    @Test
    void testSourceTypeIsNullFail() {
        SqlConfigStorageAdd configStorageAdd = mock(SqlConfigStorageAdd.class);
        ConfigRequestContext context = ConfigRequestContext.builder()
                .request(new ConfigRequest(new QueryRequest()))
                .sqlConfigCall(configStorageAdd)
                .build();
        configStorageAddDdlExecutor.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                });
    }
}
