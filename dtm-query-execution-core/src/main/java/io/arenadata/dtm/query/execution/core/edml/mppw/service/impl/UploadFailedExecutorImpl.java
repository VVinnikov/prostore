package io.arenadata.dtm.query.execution.core.edml.mppw.service.impl;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.CrashException;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class UploadFailedExecutorImpl implements EdmlUploadFailedExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final RollbackRequestContextFactory rollbackRequestContextFactory;
    private final DataSourcePluginService dataSourcePluginService;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public UploadFailedExecutorImpl(DeltaServiceDao deltaServiceDao,
                                    RollbackRequestContextFactory rollbackRequestContextFactory,
                                    DataSourcePluginService dataSourcePluginService,
                                    EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.deltaServiceDao = deltaServiceDao;
        this.rollbackRequestContextFactory = rollbackRequestContextFactory;
        this.dataSourcePluginService = dataSourcePluginService;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<Void> execute(EdmlRequestContext context) {
        return Future.future(promise -> eraseWriteOp(context)
                .compose(v -> deltaServiceDao.deleteWriteOperation(context.getSourceEntity().getSchema(),
                        context.getSysCn()))
                .onComplete(ar -> {
                    try {
                        Entity destinationEntity = context.getDestinationEntity();
                        evictQueryTemplateCacheService.evictByEntityName(destinationEntity.getSchema(), destinationEntity.getName());
                    } catch (Exception e) {
                        promise.fail(new DtmException(e));
                    }
                    if (ar.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<Void> eraseWriteOp(EdmlRequestContext context) {
        return Future.future(rbPromise -> {
            final RollbackRequestContext rollbackRequestContext =
                    rollbackRequestContextFactory.create(context);
            eraseWriteOp(rollbackRequestContext)
                    .onSuccess(rbPromise::complete)
                    .onFailure(rbPromise::fail);
        });
    }

    @Override
    public Future<Void> eraseWriteOp(RollbackRequestContext context) {
        return Future.future(rbPromise -> {
            List<Future> futures = new ArrayList<>();
            final Set<SourceType> destination = context.getRequest().getEntity().getDestination().stream()
                    .filter(dataSourcePluginService::hasSourceType)
                    .collect(Collectors.toSet());
            destination.forEach(sourceType ->
                    futures.add(Future.future(p -> dataSourcePluginService.rollback(
                            sourceType,
                            context.getMetrics(),
                            RollbackRequest.builder()
                                    .requestId(context.getRequest().getQueryRequest().getRequestId())
                                    .envName(context.getEnvName())
                                    .datamartMnemonic(context.getRequest().getDatamart())
                                    .destinationTable(context.getRequest().getDestinationTable())
                                    .sysCn(context.getRequest().getSysCn())
                                    .entity(context.getRequest().getEntity())
                                    .build())
                            .onSuccess(result -> {
                                log.debug("Rollback data in plugin [{}], datamart [{}], " +
                                                "table [{}], sysCn [{}] finished successfully",
                                        sourceType,
                                        context.getRequest().getDatamart(),
                                        context.getRequest().getDestinationTable(),
                                        context.getRequest().getSysCn());
                                p.complete();
                            })
                            .onFailure(p::fail))));
            CompositeFuture.join(futures).setHandler(ar -> {
                if (ar.succeeded()) {
                    rbPromise.complete();
                } else {
                    rbPromise.fail(
                            new CrashException("Error in rolling back data → Fatal error. Operation failed on execute and failed on undo.",
                                    ar.cause())
                    );
                }
            });
        });
    }
}
