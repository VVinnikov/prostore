package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeleteWriteOperationExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

@Slf4j
@Component
public class DeleteWriteOperationExecutorImpl extends WriteOperationSuccessExecutorImpl implements DeleteWriteOperationExecutor {

    public DeleteWriteOperationExecutorImpl(ZookeeperExecutor executor,
                                               @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return DeleteWriteOperationExecutor.class;
    }
}
