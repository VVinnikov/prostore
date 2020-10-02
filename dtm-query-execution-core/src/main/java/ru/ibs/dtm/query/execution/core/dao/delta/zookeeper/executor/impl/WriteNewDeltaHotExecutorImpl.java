package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.WriteNewDeltaHotExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaAlreadyStartedException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.InvalidDeltaNumException;
import ru.ibs.dtm.query.execution.core.dto.delta.Delta;
import ru.ibs.dtm.query.execution.core.dto.delta.HotDelta;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.util.Arrays;

@Slf4j
@Component
public class WriteNewDeltaHotExecutorImpl extends DeltaServiceDaoExecutorHelper implements WriteNewDeltaHotExecutor {

    public WriteNewDeltaHotExecutorImpl(ZookeeperExecutor executor,
                                           @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<Long> execute(String datamart, Long deltaHotNum) {
        val deltaStat = new Stat();
        Promise<Long> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
            .map(this::deserializedDelta)
            .map(delta -> {
                if (delta.getOk() != null && delta.getHot() != null) {
                    throw new DeltaAlreadyStartedException();
                }
                var deltaNum = 0L;
                var cnFrom = 0L;
                if (delta.getOk() != null) {
                    deltaNum = delta.getOk().getDeltaNum() + 1;
                    cnFrom = delta.getOk().getCnFrom() + 1;
                }
                if (deltaHotNum != null && deltaHotNum != deltaNum) {
                    throw new InvalidDeltaNumException();
                }
                val hotDelta = HotDelta.builder()
                    .deltaNum(deltaNum)
                    .cnFrom(cnFrom)
                    .cnMax(cnFrom - 1)
                    .build();
                return delta.toBuilder()
                    .hot(hotDelta)
                    .build();
            })
            .compose(delta -> executor
                .multi(getWriteNewDeltaHot(datamart, delta, deltaStat.getVersion()))
                .map(r -> delta))
            .onSuccess(delta -> {
                log.debug("write new delta hot by datamart[{}] completed successfully: [{}]", datamart, delta.getHot());
                resultPromise.complete(delta.getHot().getDeltaNum());
            })
            .onFailure(error -> {
                val errMsg = String.format("can't write new delta hot on datamart[%s], deltaHotNumber[%d]",
                    datamart,
                    deltaHotNum);
                log.error(errMsg, error);
                if (error instanceof KeeperException.NodeExistsException
                    || error instanceof KeeperException.BadVersionException) {
                    resultPromise.fail(new DeltaAlreadyStartedException(error));
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });

        return resultPromise.future();
    }

    private Iterable<Op> getWriteNewDeltaHot(String datamart,
                                             Delta delta,
                                             int deltaVersion) {
        return Arrays.asList(
            createDatamartNodeOp(getDatamartPath(datamart), "/run"),
            createDatamartNodeOp(getDatamartPath(datamart), "/block"),
            Op.setData(getDeltaPath(datamart), serializedDelta(delta), deltaVersion)
        );
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteNewDeltaHotExecutor.class;
    }
}
