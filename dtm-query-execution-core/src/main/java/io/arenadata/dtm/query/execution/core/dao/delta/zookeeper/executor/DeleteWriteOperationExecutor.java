package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;

public interface DeleteWriteOperationExecutor extends DeltaDaoExecutor {
    Future<Void> execute(String datamart, long synCn);
}
