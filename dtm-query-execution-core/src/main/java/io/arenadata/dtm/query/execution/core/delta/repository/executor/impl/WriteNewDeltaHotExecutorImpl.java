package io.arenadata.dtm.query.execution.core.delta.repository.executor.impl;

import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaServiceDaoExecutorHelper;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.WriteNewDeltaHotExecutor;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaIsNotCommittedException;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaException;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaNumIsNotNextToActualException;
import io.arenadata.dtm.query.execution.core.delta.dto.Delta;
import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Slf4j
@Component
public class WriteNewDeltaHotExecutorImpl extends DeltaServiceDaoExecutorHelper implements WriteNewDeltaHotExecutor {

    @Autowired
    public WriteNewDeltaHotExecutorImpl(ZookeeperExecutor executor,
                                        @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<Long> execute(String datamart, Long deltaHotNum) {
        val deltaStat = new Stat();
        Promise<Long> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
                .map(bytes -> bytes == null ? new Delta() : deserializedDelta(bytes))
                .map(delta -> {
                    if (delta.getHot() != null) {
                        throw new DeltaIsNotCommittedException(delta.getHot().getDeltaNum());
                    }
                    var deltaNum = 0L;
                    var cnFrom = 0L;
                    if (delta.getOk() != null) {
                        deltaNum = delta.getOk().getDeltaNum() + 1;
                        cnFrom = delta.getOk().getCnTo() + 1;
                    }
                    if (deltaHotNum != null && deltaHotNum != deltaNum) {
                        throw new DeltaNumIsNotNextToActualException(deltaHotNum.toString());
                    }
                    val hotDelta = HotDelta.builder()
                            .deltaNum(deltaNum)
                            .cnFrom(cnFrom)
                            .cnMax(cnFrom - 1)
                            .rollingBack(false)
                            .build();
                    return delta.toBuilder()
                            .hot(hotDelta)
                            .build();
                })
                .compose(delta -> executor
                        .multi(getWriteNewDeltaHot(datamart, delta, deltaStat.getVersion()))
                        .map(r -> delta))
                .onSuccess(delta -> {
                    log.debug("Write new delta hot by datamart[{}] completed successfully: [{}]", datamart, delta.getHot());
                    resultPromise.complete(delta.getHot().getDeltaNum());
                })
                .onFailure(error -> {
                    val errMsg = String.format("Can't write new delta hot on datamart[%s], deltaHotNumber[%d]",
                            datamart,
                            deltaHotNum);
                    if (error instanceof KeeperException.NodeExistsException
                            || error instanceof KeeperException.BadVersionException) {
                        resultPromise.fail(deltaHotNum == null ?
                                new DeltaIsNotCommittedException(error) :
                                new DeltaIsNotCommittedException(deltaHotNum, error));
                    } else if (error instanceof DeltaException) {
                        resultPromise.fail(error);
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
