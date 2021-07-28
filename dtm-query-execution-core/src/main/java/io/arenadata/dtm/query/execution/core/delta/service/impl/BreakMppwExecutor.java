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
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
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
        return deltaServiceDao.getDeltaWriteOperations(datamart)
                .map(ops -> ops.stream().filter(op -> op.getStatus() == WriteOperationStatus.EXECUTING.getValue()).collect(Collectors.toList()))
                .compose(ops -> sendBreakMppwEvent(datamart, ops))
                .compose(ar -> waitUntilDone(datamart));
    }

    private Future<Void> sendBreakMppwEvent(String datamart, List<DeltaWriteOp> ops) {
        return Future.future(promise -> {
            ops.forEach(op -> {
                BreakMppwContext.requestRollback(datamart, op.getSysCn(), MppwStopReason.BREAK_MPPW_RECEIVED);
                log.debug("Stop MPPW task for [{}, {}] sent", datamart, op.getSysCn());
            });
            promise.complete();
        });
    }

    private Future<Void> waitUntilDone(String datamart) {
        return Future.future(promise -> {
            if (BreakMppwContext.getNumberOfTasksByDatamart(datamart) == 0) {
                promise.complete();
                return;
            }

            periodicallyCheckTasks(datamart, promise);
        });
    }

    private void periodicallyCheckTasks(String datamart, Promise<Void> promise) {
        vertx.setTimer(rollbackStatusCallsMs, timerId -> {
            long count = BreakMppwContext.getNumberOfTasksByDatamart(datamart);
            log.debug("Break MPPW: Currently running {} tasks for datamart {}", count, datamart);
            if (count == 0) {
                vertx.cancelTimer(timerId);
                promise.complete();
            } else {
                periodicallyCheckTasks(datamart, promise);
            }
        });
    }

}
