package io.arenadata.dtm.query.execution.core.delta.repository.executor.impl;

import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaServiceDaoExecutorHelper;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.GetDeltaByNumExecutor;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaException;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaNotExistException;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaNotFoundException;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.delta.exception.NegativeDeltaNumberException;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GetDeltaByNumExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaByNumExecutor {

    @Autowired
    public GetDeltaByNumExecutorImpl(ZookeeperExecutor executor,
                                        @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<OkDelta> execute(String datamart, Long deltaNum) {
        if(deltaNum < 0) {
            return Future.failedFuture(new NegativeDeltaNumberException());
        }
        Promise<OkDelta> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart))
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getOk() == null || delta.getOk().getDeltaNum() < deltaNum) {
                    throw new DeltaNotExistException();
                }
                return delta.getOk();
            })
            .compose(okDelta -> okDelta.getDeltaNum() == deltaNum
                ? Future.succeededFuture(okDelta) : getDeltaByNumber(datamart, deltaNum))
            .onSuccess(r -> {
                log.debug("Get delta ok by datamart[{}], deltaNum[{}] completed successfully: [{}]", datamart, deltaNum, r);
                resultPromise.complete(r);
            })
            .onFailure(error -> {
                val errMsg = String.format("Can't get delta ok on datamart[%s], deltaNum[%d]",
                    datamart,
                    deltaNum);
                if (error instanceof KeeperException.NoNodeException) {
                   resultPromise.fail(new DeltaNotFoundException(error));
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    private Future<OkDelta> getDeltaByNumber(String datamart, Long deltaNum) {
        return executor.getData(getDeltaNumPath(datamart, deltaNum))
            .map(this::deserializedOkDelta);
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return GetDeltaByNumExecutor.class;
    }
}