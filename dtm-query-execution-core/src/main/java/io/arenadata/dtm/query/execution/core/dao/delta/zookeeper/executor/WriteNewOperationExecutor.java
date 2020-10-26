package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import io.vertx.core.Future;

public interface WriteNewOperationExecutor extends DeltaDaoExecutor {
    Future<Long> execute(DeltaWriteOpRequest deltaWriteOpRequest);
}
