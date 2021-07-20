package io.arenadata.dtm.query.execution.core.delta.service.impl;

import io.arenadata.dtm.query.execution.core.base.configuration.properties.RollbackDeltaProperties;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.WriteOperationStatus;
import io.arenadata.dtm.query.execution.core.edml.mppw.service.impl.BreakMppwService;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class BreakMppwExecutor {

    private final RestoreStateService restoreStateService;
    private final DeltaServiceDao deltaServiceDao;
    private final long rollbackStatusCallsMs;
    private final Vertx vertx;
    private final BreakMppwService breakMppwService;

    public BreakMppwExecutor(RestoreStateService restoreStateService,
                             ServiceDbFacade serviceDbFacade,
                             RollbackDeltaProperties rollbackDeltaProperties,
                             Vertx vertx,
                             BreakMppwService breakMppwService) {
        this.restoreStateService = restoreStateService;
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.rollbackStatusCallsMs = rollbackDeltaProperties.getRollbackStatusCallsMs();
        this.vertx = vertx;
        this.breakMppwService = breakMppwService;
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
                breakMppwService.breakMppw(datamart, op.getSysCn());
            });
            restoreStateService.restoreUpload(datamart);
            promise.complete();
        });
    }

    private Future<Void> cleanUpAndWait(String datamart, Promise<Void> promise) {
        vertx.setTimer(rollbackStatusCallsMs, timerId -> {
            deltaServiceDao.getDeltaWriteOperations(datamart)
                    .compose(result -> {
                        if (result.size() == 0) {
                            vertx.cancelTimer(timerId);
                            promise.complete();
                        }
                        cleanUpAndWait(datamart, promise);
                        return promise.future();
                    });
        });

        return promise.future();
    }

}
