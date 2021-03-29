package io.arenadata.dtm.query.execution.core.service.init;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.datasource.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

class CoreInitializationServiceTest {

    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final Set<SourceType> sourceTypes = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADQM));
    private PluginInitializationService initializationService;

    @BeforeEach
    void setUp() {
        initializationService = new CoreInitializationService(pluginService);
        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
    }

    @Test
    void executeAllPluginsSucceed() {
        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                   assertTrue(ar.succeeded());
                    verify(pluginService, times(3)).initialize(any());
                });
    }

    @Test
    void executePluginError() {
        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.failedFuture(new DtmException("")));
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(pluginService, times(3)).initialize(any());
                });
    }
}