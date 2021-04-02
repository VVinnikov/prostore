package io.arenadata.dtm.query.execution.core.delta.repository.executor.impl;

import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaServiceDaoExecutorHelper;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.GetDeltaOkExecutor;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaException;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaNotFoundException;
import io.arenadata.dtm.query.execution.core.delta.dto.Delta;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
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
public class GetDeltaOkExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaOkExecutor {

    @Autowired
    public GetDeltaOkExecutorImpl(ZookeeperExecutor executor,
                                     @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<OkDelta> execute(String datamart) {
        Promise<OkDelta> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart))
            .map(this::deserializedDelta)
            .map(Delta::getOk)
            .onSuccess(r -> {
                log.debug("get delta ok by datamart[{}] completed successfully: [{}]", datamart, r);
                resultPromise.complete(r);
            })
            .onFailure(error -> {
                val errMsg = String.format("Can't get delta ok on datamart[%s]",
                    datamart);
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
        return GetDeltaOkExecutor.class;
    }
}
