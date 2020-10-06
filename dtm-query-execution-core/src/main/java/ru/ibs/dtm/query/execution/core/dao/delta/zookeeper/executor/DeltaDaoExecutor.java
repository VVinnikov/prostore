package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor;

import org.springframework.beans.factory.annotation.Autowired;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaDaoExecutorRepository;

public interface DeltaDaoExecutor {
    @Autowired
    default void registration(DeltaDaoExecutorRepository repository) {
        repository.addExecutor(this);
    }

     Class<? extends DeltaDaoExecutor> getExecutorInterface();
}
