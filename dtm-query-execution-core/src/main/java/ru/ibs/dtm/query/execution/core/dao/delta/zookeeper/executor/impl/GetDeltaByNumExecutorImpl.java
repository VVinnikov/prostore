package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.GetDeltaByNumExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaNotExistException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaNotFoundException;
import ru.ibs.dtm.query.execution.core.dto.delta.OkDelta;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

@Slf4j
@Component
public class GetDeltaByNumExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaByNumExecutor {

    public GetDeltaByNumExecutorImpl(ZookeeperExecutor executor,
                                        @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<OkDelta> execute(String datamart, Long deltaNum) {
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
                log.debug("get delta ok by datamart[{}], deltaNum[{}] completed successfully: [{}]", datamart, deltaNum, r);
                resultPromise.complete(r);
            })
            .onFailure(error -> {
                val errMsg = String.format("can't get delta ok on datamart[%s], deltaNum[%d]",
                    datamart,
                    deltaNum);
                log.error(errMsg, error);
                if (error instanceof KeeperException.NoNodeException) {
                    resultPromise.fail(new DeltaNotFoundException(error));
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
