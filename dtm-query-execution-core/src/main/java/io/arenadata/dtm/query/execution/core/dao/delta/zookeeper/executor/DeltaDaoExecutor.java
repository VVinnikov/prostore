package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaDaoExecutorRepository;
import org.springframework.beans.factory.annotation.Autowired;

public interface DeltaDaoExecutor {
    @Autowired
    default void registration(DeltaDaoExecutorRepository repository) {
        repository.addExecutor(this);
    }

     Class<? extends DeltaDaoExecutor> getExecutorInterface();
}
