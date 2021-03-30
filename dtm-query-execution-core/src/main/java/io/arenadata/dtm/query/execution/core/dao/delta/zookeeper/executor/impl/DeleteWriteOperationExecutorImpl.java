package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeleteWriteOperationExecutor;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DeleteWriteOperationExecutorImpl extends WriteOperationSuccessExecutorImpl implements DeleteWriteOperationExecutor {

    @Autowired
    public DeleteWriteOperationExecutorImpl(ZookeeperExecutor executor,
                                            @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return DeleteWriteOperationExecutor.class;
    }
}
