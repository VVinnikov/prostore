package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;

public interface WriteOperationErrorExecutor extends DeltaDaoExecutor {
    Future<Void> execute(String datamart, long synCn);
}
