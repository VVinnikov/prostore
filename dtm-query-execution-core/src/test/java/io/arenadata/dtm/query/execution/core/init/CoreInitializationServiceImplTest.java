package io.arenadata.dtm.query.execution.core.init;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.base.service.metadata.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.InformationSchemaServiceImpl;
import io.arenadata.dtm.query.execution.core.init.service.CoreInitializationService;
import io.arenadata.dtm.query.execution.core.init.service.impl.CoreInitializationServiceImpl;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.arenadata.dtm.query.execution.core.query.verticle.starter.QueryWorkerStarter;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.rollback.service.impl.RestoreStateServiceImpl;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CoreInitializationServiceImplTest {

    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final Set<SourceType> sourceTypes = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADQM));
    private CoreInitializationService initializationService;
    private final InformationSchemaService informationSchemaService = mock(InformationSchemaServiceImpl.class);
    private final RestoreStateService restoreStateService = mock(RestoreStateServiceImpl.class);
    private final QueryWorkerStarter queryWorkerStarter = mock(QueryWorkerStarter.class);
    private final Vertx vertx = mock(Vertx.class);

    @BeforeEach
    void setUp() {
        List<Verticle> verticles = new ArrayList<>();
        initializationService = new CoreInitializationServiceImpl(pluginService,
                informationSchemaService,
                restoreStateService,
                vertx,
                queryWorkerStarter,
                verticles);
        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
    }

    @Test
    void executeAllPluginsSucceed() {
        when(informationSchemaService.initInformationSchema()).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.succeededFuture());

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
                    verify(informationSchemaService).initInformationSchema();
                    verify(restoreStateService).restoreState();
                    verify(queryWorkerStarter).start(vertx);
                });
    }

    @Test
    void executeWithRestoreError() {
        when(informationSchemaService.initInformationSchema()).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.failedFuture(new DtmException("")));
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.succeededFuture());

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
                    verify(informationSchemaService).initInformationSchema();
                    verify(restoreStateService).restoreState();
                    verify(queryWorkerStarter).start(vertx);
                });
    }

    @Test
    void executeWithInformationSchemaInitError() {
        when(informationSchemaService.initInformationSchema())
                .thenReturn(Future.failedFuture(new DtmException("")));
        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(informationSchemaService).initInformationSchema();
                    verifyNoInteractions(pluginService);
                    verifyNoInteractions(restoreStateService);
                    verifyNoInteractions(queryWorkerStarter);
                });
    }

    @Test
    void executeWithQueryWorkerStartError() {
        when(informationSchemaService.initInformationSchema())
                .thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.failedFuture(new DtmException("")));

        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(informationSchemaService).initInformationSchema();
                    verify(pluginService, times(3)).initialize(any());
                    verify(restoreStateService).restoreState();
                    verify(queryWorkerStarter).start(vertx);
                });
    }

    @Test
    void executePluginsError() {
        when(informationSchemaService.initInformationSchema()).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.failedFuture(new DtmException("")));
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(informationSchemaService).initInformationSchema();
                    verify(pluginService, times(3)).initialize(any());
                    verifyNoInteractions(restoreStateService);
                    verifyNoInteractions(queryWorkerStarter);
                });
    }

    @Test
    void executeOnePluginSuccess() {
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());
        initializationService.execute(SourceType.ADQM)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(pluginService).initialize(SourceType.ADQM);
                });
    }

    @Test
    void executeOnePluginError() {
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.failedFuture(new DtmException("")));
        initializationService.execute(SourceType.ADQM)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(pluginService).initialize(SourceType.ADQM);
                });
    }
}