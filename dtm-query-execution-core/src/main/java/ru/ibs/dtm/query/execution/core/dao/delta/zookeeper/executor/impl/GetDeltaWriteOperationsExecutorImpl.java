package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.GetDeltaWriteOperationsExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaWriteOpNotFoundException;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class GetDeltaWriteOperationsExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaWriteOperationsExecutor {

    public GetDeltaWriteOperationsExecutorImpl(ZookeeperExecutor executor,
                                               @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<List<DeltaWriteOp>> execute(String datamart) {
        Promise<List<DeltaWriteOp>> resultPromise = Promise.promise();
        executor.getChildren(getDatamartPath(datamart) + "/run")
                .compose(opPaths -> getDeltaWriteOpList(datamart, opPaths))
                .onSuccess(writeOps -> {
                    log.debug("Get delta write operations by datamart[{}] completed successfully: sysCn[{}]",
                            datamart, writeOps);
                    resultPromise.complete(writeOps);
                })
                .onFailure(error -> {
                    val errMsg = String.format("Can't get delta write operation list by datamart[%s]",
                            datamart);
                    log.error(errMsg, error);
                    if (error instanceof KeeperException.NoNodeException) {
                        resultPromise.fail(new DeltaWriteOpNotFoundException(error));
                    } else if (error instanceof DeltaException) {
                        resultPromise.fail(error);
                    } else {
                        resultPromise.fail(new DeltaException(errMsg, error));
                    }
                });
        return resultPromise.future();
    }

    private Future<List<DeltaWriteOp>> getDeltaWriteOpList(String datamart, List<String> opPaths) {
        return Future.future(promise -> {
            List<Long> opNums = opPaths.stream()
                    .map(path -> Long.parseLong(path.substring(path.lastIndexOf("/") + 1)))
                    .collect(Collectors.toList());
            CompositeFuture.join(opNums.stream()
                    .map(opNum -> getDeltaWriteOp(datamart, opNum))
                    .collect(Collectors.toList()))
                    .onSuccess(ar -> promise.complete(ar.result().list().stream()
                            .map(wrOp -> (DeltaWriteOp) wrOp)
                            .collect(Collectors.toList())))
                    .onFailure(promise::fail);
        });
    }

    private Future<DeltaWriteOp> getDeltaWriteOp(String datamart, Long opNum) {
        return executor.getData(getWriteOpPath(datamart, opNum), null, new Stat())
                .map(this::deserializeDeltaWriteOp)
                .map(deltaWriteOp -> {
                    deltaWriteOp.setSysCn(deltaWriteOp.getCnFrom() + opNum);
                    return deltaWriteOp;
                });
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return GetDeltaWriteOperationsExecutor.class;
    }
}
