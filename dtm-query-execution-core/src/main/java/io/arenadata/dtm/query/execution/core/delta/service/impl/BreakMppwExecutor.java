package io.arenadata.dtm.query.execution.core.delta.service.impl;

import io.arenadata.dtm.query.execution.core.base.configuration.properties.RollbackDeltaProperties;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopReason;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.BreakMppwContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class BreakMppwExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final long rollbackStatusCallsMs;
    private final Vertx vertx;

    public BreakMppwExecutor(ServiceDbFacade serviceDbFacade,
                             RollbackDeltaProperties rollbackDeltaProperties,
                             Vertx vertx) {
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.rollbackStatusCallsMs = rollbackDeltaProperties.getRollbackStatusCallsMs();
        this.vertx = vertx;
    }

    public Future<Void> breakMppw(String datamart) {
        Promise<Void> promise = Promise.promise();

        deltaServiceDao.getDeltaWriteOperations(datamart)
                .map(ops -> ops.stream().filter(op -> op.getStatus() == WriteOperationStatus.EXECUTING.getValue()).collect(Collectors.toList()))
                .compose(ops -> sendBreakMppwEvent(datamart, ops))
                .compose(ar -> waitUntilDone(datamart, promise));

        return promise.future();
    }

    private Future<Void> sendBreakMppwEvent(String datamart, List<DeltaWriteOp> ops) {
        return Future.future(promise -> {
            ops.forEach(op -> {
                BreakMppwContext.requestRollback(datamart, op.getSysCn(), MppwStopReason.BREAK_MPPW_RECEIVED);
            });
            promise.complete();
        });
    }

    private Future<Void> waitUntilDone(String datamart, Promise<Void> promise) {
        if (BreakMppwContext.getNumberOfTasksByDatamart(datamart) == 0) {
            promise.complete();
            return Future.succeededFuture();
        }

        periodicallyCheckTasks(datamart, promise);
        return promise.future();
    }

    private Future<Void> periodicallyCheckTasks(String datamart, Promise<Void> promise) {
        vertx.setTimer(rollbackStatusCallsMs, timerId -> {
            if (BreakMppwContext.getNumberOfTasksByDatamart(datamart) == 0) {
                vertx.cancelTimer(timerId);
                promise.complete();
            } else {
                periodicallyCheckTasks(datamart, promise);
            }
        });
        return promise.future();
    }

}
