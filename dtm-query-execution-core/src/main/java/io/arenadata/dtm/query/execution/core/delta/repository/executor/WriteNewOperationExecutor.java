package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOpRequest;
import io.vertx.core.Future;

public interface WriteNewOperationExecutor extends DeltaDaoExecutor {
    Future<Long> execute(DeltaWriteOpRequest deltaWriteOpRequest);
}
