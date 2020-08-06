package ru.ibs.dtm.query.execution.core.service.delta.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.service.delta.DeltaExecutor;
import ru.ibs.dtm.query.execution.core.service.delta.StatusEventPublisher;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.CommitDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction;

import java.time.LocalDateTime;

import static ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction.COMMIT_DELTA;

@Component
@Slf4j
public class CommitDeltaExecutor implements DeltaExecutor, StatusEventPublisher {

    private final Vertx vertx;
    private ServiceDbFacade serviceDbFacade;
    private DeltaQueryResultFactory deltaQueryResultFactory;

    @Autowired
    public CommitDeltaExecutor(ServiceDbFacade serviceDbFacade,
                               DeltaQueryResultFactory deltaQueryResultFactory,
                               @Qualifier("coreVertx") Vertx vertx) {
        this.serviceDbFacade = serviceDbFacade;
        this.vertx = vertx;
        this.deltaQueryResultFactory = deltaQueryResultFactory;
    }

    @Override
    public void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        getDeltaHotByDatamart(context)
            .compose(deltaHotRecord -> getDeltaActualBySinIdAndDatamart(context, deltaHotRecord))
            .compose(deltaActualRecord -> updateActualDelta(context, deltaActualRecord))
            .onSuccess(success -> handler.handle(Future.succeededFuture(success)))
            .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
    }

    private Future<DeltaRecord> getDeltaHotByDatamart(DeltaRequestContext context) {
        return Future.future((Promise<DeltaRecord> promiseDelta) ->
                serviceDbFacade.getDeltaServiceDao().getDeltaHotByDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic(), ar -> {
                    if (ar.succeeded()) {
                        DeltaRecord deltaHotRecord = ar.result();
                        log.debug("Found last delta: {} for datamart: {}", deltaHotRecord, context.getRequest().getQueryRequest().getDatamartMnemonic());
                        if (!deltaHotRecord.getStatus().equals(DeltaLoadStatus.IN_PROCESS)) {
                            promiseDelta.fail(new RuntimeException("Data loading for the given delta has not been completed yet!"));
                        }
                        promiseDelta.complete(deltaHotRecord);
                    } else {
                        promiseDelta.fail(ar.cause());
                    }
                }));
    }

    private Future<DeltaRecord> getDeltaActualBySinIdAndDatamart(DeltaRequestContext context, DeltaRecord deltaHotRecord) {
        return Future.future((Promise<DeltaRecord> promiseDelta) ->
                serviceDbFacade.getDeltaServiceDao().getDeltaActualBySinIdAndDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic(),
                        deltaHotRecord.getSinId() - 1, ar -> {
                            if (ar.succeeded()) {
                                DeltaRecord deltaActualRecord = ar.result();
                                log.debug("Actual delta found: {} for datamart: {}", deltaActualRecord,
                                        context.getRequest().getQueryRequest().getDatamartMnemonic());
                                if (((CommitDeltaQuery) context.getDeltaQuery()).getDeltaDateTime() != null
                                        && (deltaActualRecord.getSysDate().isAfter(((CommitDeltaQuery) context.getDeltaQuery()).getDeltaDateTime())
                                        || deltaActualRecord.getSysDate().equals(((CommitDeltaQuery) context.getDeltaQuery()).getDeltaDateTime()))) {
                                    promiseDelta.fail(new RuntimeException("The specified time is less than or equal to the time of the actual delta!"));
                                }
                                deltaHotRecord.setStatusDate(LocalDateTime.now());
                                deltaHotRecord.setSysDate(LocalDateTime.now());
                                deltaHotRecord.setStatus(DeltaLoadStatus.SUCCESS);
                                promiseDelta.complete(deltaHotRecord);
                            } else {
                                promiseDelta.fail(ar.cause());
                            }
                        }));
    }

    private Future<QueryResult> updateActualDelta(DeltaRequestContext context, DeltaRecord deltaHotRecord) {
        return Future.future((Promise<QueryResult> promiseUpdate) -> serviceDbFacade.getDeltaServiceDao().updateDelta(deltaHotRecord, ar -> {
            if (ar.succeeded()) {
                log.debug("Updated delta: {} for datamart: {}", deltaHotRecord, context.getRequest().getQueryRequest().getDatamartMnemonic());
                QueryResult res = deltaQueryResultFactory.create(context, deltaHotRecord);
                publishStatus(StatusEventCode.DELTA_CLOSE, deltaHotRecord.getDatamartMnemonic(), deltaHotRecord);
                promiseUpdate.complete(res);
            } else {
                promiseUpdate.fail(ar.cause());
            }
        }));
    }

    @Override
    public DeltaAction getAction() {
        return COMMIT_DELTA;
    }

    @Override
    public Vertx getVertx() {
        return vertx;
    }
}
