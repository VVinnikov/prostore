package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.exception.CrashException;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.WriteOperationErrorExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaNotExistException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaWriteOpNotFoundException;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

@Slf4j
@Component
public class WriteOperationErrorExecutorImpl extends DeltaServiceDaoExecutorHelper implements WriteOperationErrorExecutor {

    public WriteOperationErrorExecutorImpl(ZookeeperExecutor executor,
                                           @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<Void> execute(String datamart, long sysCn) {
        Promise<Void> resultPromise = Promise.promise();
        val opNumCnCtx = new long[1];
        executor.getData(getDeltaPath(datamart))
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getHot() == null) {
                    throw new CrashException("Delta hot not exists", new DeltaNotExistException());
                }
                opNumCnCtx[0] = sysCn - delta.getHot().getCnFrom();
                return opNumCnCtx[0];
            })
            .compose(opNum -> executor.getData(getWriteOpPath(datamart, opNum)))
            .map(this::deserializeDeltaWriteOp)
            .map(deltaWriteOp -> {
                deltaWriteOp.setStatus(2);
                return deltaWriteOp;
            })
            .map(this::serializeDeltaWriteOp)
            .compose(deltaWriteOpData -> executor.setData(getWriteOpPath(datamart, opNumCnCtx[0]), deltaWriteOpData, -1))
            .onSuccess(delta -> {
                log.debug("write delta operation \"error\" by datamart[{}], sysCn[{}] completed successfully", datamart, sysCn);
                resultPromise.complete();
            })
            .onFailure(error -> {
                val errMsg = String.format("can't write operation \"error\" on datamart[%s], sysCn[%d]",
                    datamart,
                    sysCn);
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

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteOperationErrorExecutor.class;
    }
}