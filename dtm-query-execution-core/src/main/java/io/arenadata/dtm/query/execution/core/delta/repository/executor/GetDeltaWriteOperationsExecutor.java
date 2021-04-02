package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.vertx.core.Future;

import java.util.List;

public interface GetDeltaWriteOperationsExecutor extends DeltaDaoExecutor {
    Future<List<DeltaWriteOp>> execute(String datamart);
}
