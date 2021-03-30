package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import io.vertx.core.Future;

import java.util.List;

public interface GetDeltaWriteOperationsExecutor extends DeltaDaoExecutor {
    Future<List<DeltaWriteOp>> execute(String datamart);
}
