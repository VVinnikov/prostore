package io.arenadata.dtm.query.execution.core.base.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.base.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.MatViewSyncProperties;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.plugin.exception.SuitablePluginNotExistsException;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
public class MaterializedViewSyncService {

    private final DataSourcePluginService dataSourcePluginService;
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    private final DeltaServiceDao deltaServiceDao;
    private final EntityDao entityDao;
    private final Vertx vertx;
    private final long retryCount;
    private final long periodMs;
    private final AppConfiguration appConfiguration;

    public MaterializedViewSyncService(DataSourcePluginService dataSourcePluginService,
                                       CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                                       DeltaServiceDao deltaServiceDao,
                                       EntityDao entityDao,
                                       @Qualifier("coreVertx") Vertx vertx,
                                       MatViewSyncProperties matViewSyncProperties,
                                       AppConfiguration appConfiguration) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.materializedViewCacheService = materializedViewCacheService;
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = entityDao;
        this.vertx = vertx;
        this.retryCount = matViewSyncProperties.getRetryCount();
        this.periodMs = matViewSyncProperties.getPeriodMs();
        this.appConfiguration = appConfiguration;
    }

    public long startPeriodicalSync() {
        return vertx.setTimer(periodMs, timerId -> {
            Map<EntityKey, MaterializedViewCacheValue> cacheMap = materializedViewCacheService.asMap();
            List<Future> futures = new ArrayList<>();
            cacheMap.forEach((key, value) -> futures.add(getSyncFuture(key, value)));
            CompositeFuture.join(futures)
                    .onComplete(ar -> startPeriodicalSync());
        });
    }

    private Future<Void> getSyncFuture(EntityKey key, MaterializedViewCacheValue value) {
        return Future.future(promise -> {
            val datamart = key.getDatamartName();
            val origUUID = value.getUuid();
            val entity = value.getEntity();
            if (origUUID == null) {
                materializedViewCacheService.remove(key);
                promise.complete();
            } else {
                isReadyForSync(datamart, value.getStatus(), entity.getMaterializedDeltaNum(), value.getFailsCount())
                        .onSuccess(isReady -> {
                            if (isReady) {
                                runSync(datamart, value, origUUID)
                                        .onSuccess(v -> {
                                            log.info("Materialized view {} synchronized", entity.getNameWithSchema());
                                            promise.complete();
                                        })
                                        .onFailure(error -> {
                                            log.error("Failed to sync materialized view {}, fails count {}/{}", entity.getNameWithSchema(), value.getFailsCount() + 1, retryCount, error);
                                            if (origUUID.equals(value.getUuid())) {
                                                value.incrementFailsCount();
                                                value.setStatus(0);
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
            }
        });
    }

    private Future<Void> updateEntity(long deltaNum, MaterializedViewCacheValue cacheValue) {
        val entity = cacheValue.getEntity();
        val oldDeltaNum = entity.getMaterializedDeltaNum();
        if (oldDeltaNum != deltaNum) {
            entity.setMaterializedDeltaNum(deltaNum);
            return entityDao.updateEntity(entity)
                    .map(v -> {
                        cacheValue.setEntity(entity);
                        cacheValue.setFailsCount(0);
                        cacheValue.setStatus(0);
                        return v;
                    });
        }
        return Future.failedFuture(new DtmException(String.format("DeltaNum for materialized view %s has not been changed; old value [%d], new value [%d]",
                entity.getNameWithSchema(), oldDeltaNum, deltaNum)));
    }

    private Future<Boolean> isReadyForSync(String datamart, int matViewStatus, Long matViewDeltaNum, long matViewRetryCount) {
        return deltaServiceDao.getDeltaOk(datamart)
                .map(okDelta -> ((okDelta != null && okDelta.getDeltaNum() >= 0)
                        && matViewStatus == 0
                        && (matViewDeltaNum == null || matViewDeltaNum < okDelta.getDeltaNum())
                        && matViewRetryCount < retryCount));

    }

    private Future<Void> runSync(String datamart, MaterializedViewCacheValue value, UUID origUUID) {
        value.setStatus(1);
        return synchronize(datamart, value.getEntity())
                .compose(deltaNum -> origUUID.equals(value.getUuid()) ? updateEntity(deltaNum, value) : Future.succeededFuture());
    }

    private Future<Long> synchronize(String datamart, Entity matViewEntity) {
        if (!dataSourcePluginService.hasSourceType(matViewEntity.getMaterializedDataSource())) {
            throw new SuitablePluginNotExistsException();
        }
        val uuid = UUID.randomUUID();
        return dataSourcePluginService.synchronize(matViewEntity.getMaterializedDataSource(),
                createRequestMetrics(uuid), new SynchronizeRequest(uuid, appConfiguration.getEnvName(), datamart, matViewEntity));
    }

    private RequestMetrics createRequestMetrics(UUID uuid) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(appConfiguration.dtmSettings().getTimeZone()))
                .requestId(uuid)
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }

}
