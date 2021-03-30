package io.arenadata.dtm.query.execution.core.service.init.impl;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.init.CoreInitializationService;
import io.arenadata.dtm.query.execution.core.service.metadata.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.rollback.RestoreStateService;
import io.arenadata.dtm.query.execution.core.verticle.starter.QueryWorkerStarter;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service("coreInitializationService")
@Slf4j
public class CoreInitializationServiceImpl implements CoreInitializationService {

    private final DataSourcePluginService sourcePluginService;
    private final InformationSchemaService informationSchemaService;
    private final RestoreStateService restoreStateService;
    private final Vertx vertx;
    private final QueryWorkerStarter queryWorkerStarter;
    private final List<Verticle> verticles;

    @Autowired
    public CoreInitializationServiceImpl(DataSourcePluginService sourcePluginService,
                                         InformationSchemaService informationSchemaService,
                                         RestoreStateService restoreStateService,
                                         @Qualifier("coreVertx") Vertx vertx,
                                         QueryWorkerStarter queryWorkerStarter,
                                         List<Verticle> verticles) {
        this.sourcePluginService = sourcePluginService;
        this.informationSchemaService = informationSchemaService;
        this.restoreStateService = restoreStateService;
        this.vertx = vertx;
        this.queryWorkerStarter = queryWorkerStarter;
        this.verticles = verticles;
    }

    @Override
    public Future<Void> execute() {
        return informationSchemaService.createInformationSchemaViews()
                .compose(v -> deployVerticles(vertx, verticles))
                .compose(v -> initPlugins())
                .compose(v -> {
                    restoreStateService.restoreState()
                            .onFailure(fail -> log.error("Error in restoring state", fail));
                    return queryWorkerStarter.start(vertx);
                });
    }

    private Future<Object> deployVerticles(Vertx vertx, Collection<Verticle> verticles) {
        log.info("Verticals found: {}", verticles.size());
        return CompositeFuture.join(verticles.stream()
                .map(verticle -> Future.future(p -> vertx.deployVerticle(verticle, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Vertical '{}' deployed successfully", verticle.getClass().getName());
                        p.complete();
                    } else {
                        log.error("Vertical deploy error", ar.cause());
                        p.fail(ar.cause());
                    }
                })))
                .collect(Collectors.toList()))
                .mapEmpty();
    }

    private Future<Void> initPlugins() {
        return Future.future(promise -> {
            Set<SourceType> sourceTypes = sourcePluginService.getSourceTypes();
            CompositeFuture.join(sourceTypes.stream()
                    .map(sourcePluginService::initialize)
                    .collect(Collectors.toList()))
                    .onSuccess(s -> {
                        log.info("Plugins: {} initialized successfully", sourceTypes);
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    @Override
    public Future<Void> execute(SourceType sourceType) {
        return Future.future(promise -> sourcePluginService.initialize(sourceType)
                .onSuccess(success -> log.info("Plugin: {} initialized successfully", sourceType))
                .onFailure(promise::fail));
    }
}
