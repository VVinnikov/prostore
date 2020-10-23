package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;

public interface WriteNewDeltaHotExecutor extends DeltaDaoExecutor {
    Future<Long> execute(String datamart, Long deltaHotNum);
}
