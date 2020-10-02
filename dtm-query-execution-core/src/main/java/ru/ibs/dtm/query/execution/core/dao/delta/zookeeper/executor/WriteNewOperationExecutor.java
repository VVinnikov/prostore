package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;

public interface WriteNewOperationExecutor extends DeltaDaoExecutor {
    Future<Long> execute(DeltaWriteOpRequest deltaWriteOpRequest);
}
