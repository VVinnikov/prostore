package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.GetDeltaHotExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaNotFoundException;
import ru.ibs.dtm.query.execution.core.dto.delta.Delta;
import ru.ibs.dtm.query.execution.core.dto.delta.HotDelta;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

@Slf4j
@Component
public class GetDeltaHotExecutorImpl extends DeltaServiceDaoExecutorHelper implements GetDeltaHotExecutor {

    public GetDeltaHotExecutorImpl(ZookeeperExecutor executor,
                                      @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<HotDelta> execute(String datamart) {
        val deltaStat = new Stat();
        Promise<HotDelta> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
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
