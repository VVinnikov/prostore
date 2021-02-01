package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.operation.WriteOpFinish;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.delta.query.RollbackDeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.request.RollbackRequest;
import io.arenadata.dtm.query.execution.core.dto.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.core.exception.delta.DeltaAlreadyIsRollingBackException;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.StatusEventPublisher;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.ROLLBACK_DELTA;

@Component
@Slf4j
public class RollbackDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private final EdmlUploadFailedExecutor edmlUploadFailedExecutor;
    private final DeltaQueryResultFactory deltaQueryResultFactory;
    private final DeltaServiceDao deltaServiceDao;
    private final Vertx vertx;
    private final EntityDao entityDao;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public RollbackDeltaExecutor(EdmlUploadFailedExecutor edmlUploadFailedExecutor,
                                 ServiceDbFacade serviceDbFacade,
                                 @Qualifier("beginDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                                 @Qualifier("coreVertx") Vertx vertx,
                                 EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.edmlUploadFailedExecutor = edmlUploadFailedExecutor;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.vertx = vertx;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return executeInternal(deltaQuery);
    }

    private Future<QueryResult> executeInternal(DeltaQuery deltaQuery) {
        return Future.future(promise ->
                deltaServiceDao.writeDeltaError(deltaQuery.getDatamart(), null)
                .otherwise(this::skipDeltaAlreadyIsRollingBackError)
                .compose(v -> deltaServiceDao.getDeltaHot(deltaQuery.getDatamart()))
                .compose(hotDelta -> {
                    try {
                        evictQueryTemplateCacheService.evictByDatamartName(deltaQuery.getDatamart());
                    } catch (Exception e) {
                        return Future.failedFuture(new DtmException("Evict cache error"));
                    }
                    return Future.succeededFuture(hotDelta);
                })
                .compose(hotDelta -> rollbackTables((RollbackDeltaQuery) deltaQuery, hotDelta)
                        .map(v -> hotDelta))
                .compose(hotDelta -> deltaServiceDao.deleteDeltaHot(deltaQuery.getDatamart())
                        .map(hotDelta.getDeltaNum()))
                .onSuccess(deltaNum -> {
                    try {
                        publishStatus(StatusEventCode.DELTA_CANCEL, deltaQuery.getDatamart(), deltaNum);
                        val res = deltaQueryResultFactory.create(getDeltaRecord(deltaQuery.getDatamart(),
                                deltaNum));
                        promise.complete(res);
                    } catch (Exception e) {
                        promise.fail(new DtmException(String.format("Can't publish result of delta rollback by datamart [%s]",
                                deltaQuery.getDatamart()), e));
                    }
                })
                .onFailure(error -> promise.fail(new DtmException(String.format("Can't rollback delta by datamart [%s]",
                        deltaQuery.getDatamart()), error))));
    }

    @SneakyThrows
    private Void skipDeltaAlreadyIsRollingBackError(Throwable error) {
        if (error instanceof DeltaAlreadyIsRollingBackException) {
            return null;
        } else {
            throw error;
        }
    }

    private Future<Void> rollbackTables(RollbackDeltaQuery deltaQuery,
                                        HotDelta hotDelta) {
        val operationsFinished = hotDelta.getWriteOperationsFinished();
        return operationsFinished != null ?
                getRollbackTablesFuture(deltaQuery, operationsFinished) : Future.succeededFuture();
    }

    private Future<Void> getRollbackTablesFuture(RollbackDeltaQuery deltaQuery,
                                                 List<WriteOpFinish> operationsFinished) {
        Future<Void> executingFuture = Future.succeededFuture();
        for (WriteOpFinish writeOpFinish : operationsFinished) {
            executingFuture = executingFuture.compose(v -> rollbackTable(deltaQuery, writeOpFinish));
        }
        return executingFuture;
    }

    private Future<Void> rollbackTable(RollbackDeltaQuery deltaQuery, WriteOpFinish writeOpFinish) {
        return entityDao.getEntity(deltaQuery.getDatamart(), writeOpFinish.getTableName())
                .compose(entity -> rollbackTableWriteOperations(deltaQuery, writeOpFinish, entity));
    }

    private Future<Void> rollbackTableWriteOperations(RollbackDeltaQuery deltaQuery,
                                                      WriteOpFinish writeOpFinish,
                                                      Entity entity) {
        Future<Void> executingFuture = Future.succeededFuture();
        List<RollbackRequestContext> rollbackRequestContexts = writeOpFinish.getCnList().stream()
                .map(sysCn -> RollbackRequest.builder()
                        .destinationTable(entity.getName())
                        .queryRequest(deltaQuery.getRequest())
                        .datamart(deltaQuery.getDatamart())
                        .entity(entity)
                        .sysCn(sysCn)
                        .build())
                .map(rollbackRequest -> new RollbackRequestContext(deltaQuery.getRequestMetrics(),
                        deltaQuery.getEnvName(),
                        rollbackRequest,
                        deltaQuery.getSqlNode()
                        ))
                .collect(Collectors.toList());
        for (RollbackRequestContext rollbackRequestContext : rollbackRequestContexts) {
            executingFuture = executingFuture.compose(v -> edmlUploadFailedExecutor.eraseWriteOp(rollbackRequestContext));
        }
        return executingFuture;
    }

    private DeltaRecord getDeltaRecord(String datamart, long deltaNum) {
        return DeltaRecord.builder()
                .datamart(datamart)
                .deltaNum(deltaNum)
                .build();
    }

    @Override
    public DeltaAction getAction() {
        return ROLLBACK_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
