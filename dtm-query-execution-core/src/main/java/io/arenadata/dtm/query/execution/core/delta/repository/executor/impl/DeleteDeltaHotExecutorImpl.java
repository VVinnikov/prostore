package io.arenadata.dtm.query.execution.core.delta.repository.executor.impl;

import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeleteDeltaHotExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaServiceDaoExecutorHelper;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaException;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DeleteDeltaHotExecutorImpl extends DeltaServiceDaoExecutorHelper implements DeleteDeltaHotExecutor {

    public DeleteDeltaHotExecutorImpl(ZookeeperExecutor executor,
                                      @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<Void> execute(String datamart) {
        val deltaStat = new Stat();
        Promise<Void> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
            .map(this::deserializedDelta)
            .map(delta -> {
                delta.setHot(null);
                return serializedDelta(delta);
            })
            .compose(deltaData -> executor.setData(getDeltaPath(datamart), deltaData, deltaStat.getVersion()))
            .onSuccess(r -> {
                log.debug("Deletion delta hot by datamart[{}] completed successfully", datamart);
                resultPromise.complete();
            })
            .onFailure(error -> {
                val errMsg = String.format("Can't delete delta hot on datamart[%s]",
                    datamart);
                if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return DeleteDeltaHotExecutor.class;
    }
}
