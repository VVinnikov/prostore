package io.arenadata.dtm.query.execution.core.delta.service.impl;

import io.arenadata.dtm.common.eventbus.DataTopic;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.RollbackDeltaProperties;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.dto.request.BreakMppwRequest;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class BreakMppwService {

    private final Vertx vertx;
    private final RestoreStateService restoreStateService;
    private final DeltaServiceDao deltaServiceDao;
    private final long rollbackStatusCallsMs;

    public BreakMppwService(Vertx vertx,
                            RestoreStateService restoreStateService,
                            ServiceDbFacade serviceDbFacade,
                            RollbackDeltaProperties rollbackDeltaProperties) {
        this.vertx = vertx;
        this.restoreStateService = restoreStateService;
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.rollbackStatusCallsMs = rollbackDeltaProperties.getRollbackStatusCallsMs();
    }

    public Future<Void> breakMppw(String datamart) {
        Promise<Void> promise = Promise.promise();

        deltaServiceDao.getDeltaWriteOperations(datamart)
                .map(ops -> ops.stream().filter(op -> op.getStatus() == WriteOperationStatus.EXECUTING.getValue()).collect(Collectors.toList()))
                .compose(ops -> sendBreakMppwEvent(datamart, ops))
                .compose(ar -> cleanUpAndWait(datamart, promise));

        return promise.future();
    }

    private Future<String> sendBreakMppwEvent(String datamart, List<DeltaWriteOp> ops) {
        return Future.future(promise -> {
            ops.forEach(op -> {
                BreakMppwRequest breakMppwRequest = BreakMppwRequest
                        .builder()
                        .datamart(datamart)
                        .sysCn(op.getSysCn())
                        .build();
                vertx.eventBus().send(DataTopic.BREAK_MPPW_TASK.getValue(), breakMppwRequest.asString());
            });
            promise.complete();
        });
    }

    private Future<Void> cleanUpAndWait(String datamart, Promise<Void> promise) {
        restoreStateService.restoreUpload(datamart);
        vertx.setPeriodic(rollbackStatusCallsMs, timerId -> {
            deltaServiceDao.getDeltaWriteOperations(datamart)
                    .compose(result -> {
                        if (result.size() == 0) {
                            vertx.cancelTimer(timerId);
                            promise.complete();
                        }
                        return promise.future();
                    });
        });

        return promise.future();
    }

}
