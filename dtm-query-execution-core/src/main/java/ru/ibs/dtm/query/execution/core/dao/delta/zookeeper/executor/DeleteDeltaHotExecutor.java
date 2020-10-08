package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;

public interface DeleteDeltaHotExecutor extends DeltaDaoExecutor {
    Future<Void> execute(String datamart);
}
