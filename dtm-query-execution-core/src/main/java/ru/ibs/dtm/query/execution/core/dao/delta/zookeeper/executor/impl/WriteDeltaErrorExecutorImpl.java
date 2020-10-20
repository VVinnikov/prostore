package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.WriteDeltaErrorExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaHotNotStartedException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaNotFinishedException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.InvalidDeltaNumException;
import ru.ibs.dtm.query.execution.core.dto.delta.Delta;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.util.Arrays;

@Slf4j
@Component
public class WriteDeltaErrorExecutorImpl extends DeltaServiceDaoExecutorHelper implements WriteDeltaErrorExecutor {

    public WriteDeltaErrorExecutorImpl(ZookeeperExecutor executor,
                                       @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<Void> execute(String datamart, Long deltaHotNum) {
        val deltaStat = new Stat();
        Promise<Void> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getHot() == null) {
                    throw new DeltaHotNotStartedException();
                } else if (deltaHotNum != null && deltaHotNum != delta.getHot().getDeltaNum()) {
                    throw new InvalidDeltaNumException();
                }
                delta.getHot().setCnTo(-1L);
                delta.getHot().setRollingBack(true);
                return delta;
            })
            .compose(delta -> executor.multi(getErrorOps(datamart, delta, deltaStat.getVersion())))
            .onSuccess(r -> {
                log.debug("write delta error by datamart[{}], deltaNum[{}] completed successfully", datamart, deltaHotNum);
                resultPromise.complete();
            })
            .onFailure(error -> {
                val errMsg = String.format("can't write delta error on datamart[%s], deltaNum[%s]",
                    datamart,
                    deltaHotNum);
                log.error(errMsg, error);
                if (error instanceof KeeperException) {
                    if (error instanceof KeeperException.NotEmptyException) {
                        resultPromise.fail(new DeltaNotFinishedException(error));
                    } else {
                        resultPromise.fail(new DeltaException(errMsg, error));
                    }
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    private Iterable<Op> getErrorOps(String datamart, Delta delta, int deltaVersion) {
        return Arrays.asList(
            Op.delete(getDatamartPath(datamart) + "/run", -1),
            Op.delete(getDatamartPath(datamart) + "/block", -1),
            Op.setData(getDeltaPath(datamart), serializedDelta(delta), deltaVersion)
        );
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteDeltaErrorExecutor.class;
    }
}
