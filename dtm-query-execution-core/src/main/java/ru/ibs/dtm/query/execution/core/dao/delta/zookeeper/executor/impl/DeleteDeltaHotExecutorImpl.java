package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeleteDeltaHotExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

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
                log.debug("deletion delta hot by datamart[{}] completed successfully", datamart);
                resultPromise.complete();
            })
            .onFailure(error -> {
                val errMsg = String.format("can't delete delta hot on datamart[%s]",
                    datamart);
                log.error(errMsg, error);
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
