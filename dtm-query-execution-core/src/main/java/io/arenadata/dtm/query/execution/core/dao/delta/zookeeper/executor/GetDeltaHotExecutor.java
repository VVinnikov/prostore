package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.vertx.core.Future;

public interface GetDeltaHotExecutor extends DeltaDaoExecutor {
    Future<HotDelta> execute(String datamart);
}
