package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.DeltaAlreadyIsRollingBackException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.operation.WriteOpFinish;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.StatusEventPublisher;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.vertx.core.*;
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

    @Autowired
    public RollbackDeltaExecutor(EdmlUploadFailedExecutor edmlUploadFailedExecutor,
                                 ServiceDbFacade serviceDbFacade,
                                 @Qualifier("beginDeltaQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory,
                                 @Qualifier("coreVertx") Vertx vertx) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.edmlUploadFailedExecutor = edmlUploadFailedExecutor;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
        this.vertx = vertx;
    }

    @Override
    public void execute(DeltaQuery deltaQuery, Handler<AsyncResult<QueryResult>> handler) {
        val queryRequest = deltaQuery.getRequest();
        val datamart = deltaQuery.getDatamart();
        deltaServiceDao.writeDeltaError(datamart, null)
                .otherwise(this::skipDeltaAlreadyIsRollingBackError)
                .compose(v -> deltaServiceDao.getDeltaHot(datamart))
                .compose(hotDelta -> rollbackTables(datamart, hotDelta, queryRequest)
                        .map(v -> hotDelta))
                .compose(hotDelta -> deltaServiceDao.deleteDeltaHot(datamart)
                        .map(hotDelta.getDeltaNum()))
                .onSuccess(deltaNum -> {
                    try {
                        publishStatus(StatusEventCode.DELTA_CANCEL, datamart, deltaNum);
                        val res = deltaQueryResultFactory.create(getDeltaRecord(datamart, deltaNum));
                        handler.handle(Future.succeededFuture(res));
                    } catch (Exception e) {
                        val errMsg = String.format("Can't publish result of delta rollback by datamart [%s]: %s", datamart, e.getMessage());
                        log.error(errMsg);
                        handler.handle(Future.failedFuture(new RuntimeException(errMsg)));
                    }
                })
                .onFailure(error -> {
                    val errMsg = String.format("Can't rollback delta by datamart [%s]: %s", datamart, error.getMessage());
                    log.error(errMsg);
                    handler.handle(Future.failedFuture(new RuntimeException(errMsg)));
                });
    }

    @SneakyThrows
    private Void skipDeltaAlreadyIsRollingBackError(Throwable error) {
        if (error instanceof DeltaAlreadyIsRollingBackException) {
            return null;
        } else {
            throw error;
        }
    }

    private Future<Void> rollbackTables(String datamart,
                                        HotDelta hotDelta,
                                        QueryRequest queryRequest) {
        val operationsFinished = hotDelta.getWriteOperationsFinished();
        return operationsFinished != null ?
                getRollbackTablesFuture(datamart, operationsFinished, queryRequest) : Future.succeededFuture();
    }

    private Future<Void> getRollbackTablesFuture(String datamart,
                                                 List<WriteOpFinish> operationsFinished,
                                                 QueryRequest queryRequest) {
        Future<Void> executingFuture = Future.succeededFuture();
        operationsFinished.stream()
                .forEach(writeOpFinish -> executingFuture.compose(v -> rollbackTable(datamart, writeOpFinish, queryRequest)));
        return executingFuture;
    }

    private Future<Void> rollbackTable(String datamart,
                                       WriteOpFinish writeOpFinish,
                                       QueryRequest queryRequest) {
        return entityDao.getEntity(datamart, writeOpFinish.getTableName())
                .compose(entity -> rollbackTableWriteOperations(datamart, writeOpFinish, entity, queryRequest));
    }

    private Future<Void> rollbackTableWriteOperations(String datamart, WriteOpFinish writeOpFinish, Entity entity, QueryRequest queryRequest) {
        Future<Void> executingFuture = Future.succeededFuture();
        writeOpFinish.getCnList().stream()
                .map(sysCn -> RollbackRequest.builder()
                        .destinationTable(entity.getName())
                        .queryRequest(queryRequest)
                        .datamart(datamart)
                        .entity(entity)
                        .sysCn(sysCn)
                        .build())
                .map(RollbackRequestContext::new)
                .forEach(rollbackRequestContext -> {
                    executingFuture.compose(v -> edmlUploadFailedExecutor.eraseWriteOp(rollbackRequestContext));
                });
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
