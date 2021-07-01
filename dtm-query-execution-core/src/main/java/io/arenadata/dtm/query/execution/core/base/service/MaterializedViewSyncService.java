package io.arenadata.dtm.query.execution.core.base.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.base.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.MatViewSyncProperties;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewSyncStatus;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.plugin.exception.SuitablePluginNotExistsException;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@Service
public class MaterializedViewSyncService {

    private final DataSourcePluginService dataSourcePluginService;
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    private final DeltaServiceDao deltaServiceDao;
    private final EntityDao entityDao;
    private final DefinitionService<SqlNode> definitionService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final Vertx vertx;
    private final long retryCount;
    private final long periodMs;
    private final AppConfiguration appConfiguration;

    public MaterializedViewSyncService(DataSourcePluginService dataSourcePluginService,
                                       CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                                       DeltaServiceDao deltaServiceDao,
                                       EntityDao entityDao,
                                       @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                       LogicalSchemaProvider logicalSchemaProvider,
                                       @Qualifier("coreVertx") Vertx vertx,
                                       MatViewSyncProperties matViewSyncProperties,
                                       AppConfiguration appConfiguration) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.materializedViewCacheService = materializedViewCacheService;
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = entityDao;
        this.definitionService = definitionService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.vertx = vertx;
        this.retryCount = matViewSyncProperties.getRetryCount();
        this.periodMs = matViewSyncProperties.getPeriodMs();
        this.appConfiguration = appConfiguration;
    }

    public long startPeriodicalSync() {
        log.info("Materialized view synchronization timer started");
        return vertx.setTimer(periodMs, timerId -> {
            List<Future> futures = new ArrayList<>();
            materializedViewCacheService.forEach((key, value) -> futures.add(getSyncFuture(key, value)));
            CompositeFuture.join(futures)
                    .onComplete(ar -> startPeriodicalSync());
        });
    }

    private Future<Void> getSyncFuture(EntityKey key, MaterializedViewCacheValue value) {
        return Future.future(promise -> {
            val datamart = key.getDatamartName();
            val origUUID = value.getUuid();
            val entity = value.getEntity();
            if (value.isMarkedForDeletion()) {
                materializedViewCacheService.remove(key);
                promise.complete();
                return;
            }
            isReadyForSync(datamart, value.getStatus(), entity.getMaterializedDeltaNum(), value.getFailsCount())
                    .onSuccess(isReady -> {
                        if (isReady) {
                            log.info("Started sync process for {}", value);
                            runSync(datamart, value, origUUID)
                                    .onSuccess(v -> {
                                        log.info("Materialized view {} synchronized", entity.getNameWithSchema());
                                        promise.complete();
                                    })
                                    .onFailure(error -> {
                                        log.error("Failed to sync materialized view {}, fails count {}/{}", entity.getNameWithSchema(), value.getFailsCount() + 1, retryCount, error);
                                        if (origUUID.equals(value.getUuid())) {
                                            value.incrementFailsCount();
                                            value.setStatus(MaterializedViewSyncStatus.READY);
                                        }
                                        promise.complete();
                                    });
                        } else {
                            promise.complete();
                        }
                    })
                    .onFailure(error -> {
                        log.warn("Can't start materialized view sync cause can't get delta ok for datamart {}", datamart, error);
                        promise.complete();
                    });
        });
    }

    private Future<Boolean> isReadyForSync(String datamart, MaterializedViewSyncStatus matViewStatus, Long matViewDeltaNum, long matViewRetryCount) {
        if (MaterializedViewSyncStatus.READY != matViewStatus || matViewRetryCount >= retryCount) {
            return Future.succeededFuture(false);
        }
        return deltaServiceDao.getDeltaOk(datamart)
                .map(okDelta -> ((okDelta != null && okDelta.getDeltaNum() >= 0L)
                        && (matViewDeltaNum == null || matViewDeltaNum < okDelta.getDeltaNum())));
    }

    private Future<Void> runSync(String datamart, MaterializedViewCacheValue value, UUID origUUID) {
        value.setStatus(MaterializedViewSyncStatus.RUN);
        return synchronize(datamart, value.getEntity())
                .compose(deltaNum -> origUUID.equals(value.getUuid()) ? updateEntity(deltaNum, value) : Future.succeededFuture());
    }

    private Future<Long> synchronize(String datamart, Entity matViewEntity) {
        return Future.future(promise -> {
            if (!dataSourcePluginService.hasSourceType(matViewEntity.getMaterializedDataSource())) {
                throw new SuitablePluginNotExistsException();
            }

            val uuid = UUID.randomUUID();
            preparePluginContext(datamart, matViewEntity)
                    .compose(context -> dataSourcePluginService.synchronize(matViewEntity.getMaterializedDataSource(),
                            createRequestMetrics(uuid), prepareRequest(uuid, datamart, matViewEntity, context)))
                    .onComplete(promise);
        });
    }

    private SynchronizeRequest prepareRequest(UUID uuid, String datamart, Entity matViewEntity, SynchronizePluginContext context) {
        return new SynchronizeRequest(uuid, appConfiguration.getEnvName(), datamart,
                context.querySchema, matViewEntity, context.viewQuery, context.okDelta.getDeltaNum(), context.okDelta.getCnTo());
    }

    private Future<SynchronizePluginContext> preparePluginContext(String datamart, Entity matViewEntity) {
        return Future.future(promise -> {
            val sqlNode = definitionService.processingQuery(matViewEntity.getViewQuery());
            val synchronizeRequest = new SynchronizePluginContext(sqlNode);
            logicalSchemaProvider.getSchemaFromQuery(sqlNode, datamart)
                    .compose(datamarts -> {
                        synchronizeRequest.querySchema = datamarts;
                        return deltaServiceDao.getDeltaByNum(datamart, getDeltaNumToBe(matViewEntity));
                    })
                    .onSuccess(okDelta -> {
                        synchronizeRequest.okDelta = okDelta;
                        promise.complete(synchronizeRequest);
                    })
                    .onFailure(promise::fail);
        });
    }

    private Long getDeltaNumToBe(Entity matViewEntity) {
        if (matViewEntity.getMaterializedDeltaNum() == null) {
            return 0L;
        }

        return matViewEntity.getMaterializedDeltaNum() + 1L;
    }

    private Future<Void> updateEntity(long deltaNum, MaterializedViewCacheValue cacheValue) {
        val entity = cacheValue.getEntity();
        val oldDeltaNum = entity.getMaterializedDeltaNum();
        if (!Objects.equals(oldDeltaNum, deltaNum)) {
            entity.setMaterializedDeltaNum(deltaNum);
            return entityDao.updateEntity(entity)
                    .map(v -> {
                        cacheValue.setFailsCount(0);
                        cacheValue.setStatus(MaterializedViewSyncStatus.READY);
                        return v;
                    });
        }
        return Future.failedFuture(new DtmException(String.format("DeltaNum for materialized view %s has not been changed; old value [%d], new value [%d]",
                entity.getNameWithSchema(), oldDeltaNum, deltaNum)));
    }

    private RequestMetrics createRequestMetrics(UUID uuid) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(appConfiguration.dtmSettings().getTimeZone()))
                .requestId(uuid)
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }

    private static class SynchronizePluginContext {
        private final SqlNode viewQuery;
        private List<Datamart> querySchema;
        private OkDelta okDelta;

        private SynchronizePluginContext(SqlNode viewQuery) {
            this.viewQuery = viewQuery;
        }
    }
}
