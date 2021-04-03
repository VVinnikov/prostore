package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaDaoExecutorRepository;
import org.springframework.beans.factory.annotation.Autowired;

public interface DeltaDaoExecutor {
    @Autowired
    default void registration(DeltaDaoExecutorRepository repository) {
        repository.addExecutor(this);
    }

     Class<? extends DeltaDaoExecutor> getExecutorInterface();
}
