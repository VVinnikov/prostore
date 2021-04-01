package io.arenadata.dtm.query.execution.core.delta.repository.zookeeper;

import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaDaoExecutor;

public interface DeltaDaoExecutorRepository {
    <T extends DeltaDaoExecutor> T getExecutor(Class<T> executorInterface);

    <T extends DeltaDaoExecutor> void addExecutor(T executor);
}
