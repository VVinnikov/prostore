package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper;

import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;

public interface DeltaDaoExecutorRepository {
    <T extends DeltaDaoExecutor> T getExecutor(Class<T> executorInterface);

    <T extends DeltaDaoExecutor> void addExecutor(T executor);
}
