package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import io.arenadata.dtm.query.execution.core.delta.dto.HotDelta;
import io.vertx.core.Future;

public interface GetDeltaHotExecutor extends DeltaDaoExecutor {
    Future<HotDelta> execute(String datamart);
}
