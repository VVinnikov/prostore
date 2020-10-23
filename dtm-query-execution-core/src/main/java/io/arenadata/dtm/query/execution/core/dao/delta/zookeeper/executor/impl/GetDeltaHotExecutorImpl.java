package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.GetDeltaHotExecutor;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.DeltaException;
import io.arenadata.dtm.query.execution.core.dao.exception.delta.DeltaNotFoundException;
import io.arenadata.dtm.query.execution.core.dto.delta.Delta;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GetDeltaHotExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaHotExecutor {

    public GetDeltaHotExecutorImpl(ZookeeperExecutor executor,
                                   @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<HotDelta> execute(String datamart) {
        Promise<HotDelta> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart))
            .map(this::deserializedDelta)
            .map(Delta::getHot)
            .onSuccess(r -> {
                log.debug("get delta hot by datamart[{}] completed successfully: [{}]", datamart, r);
                resultPromise.complete(r);
            })
            .onFailure(error -> {
                val errMsg = String.format("can't get delta hot on datamart[%s]",
                    datamart);
                log.error(errMsg, error);
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

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return GetDeltaHotExecutor.class;
    }
}
