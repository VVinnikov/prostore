package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.vertx.core.Future;

public interface GetDeltaByNumExecutor extends DeltaDaoExecutor {
    Future<OkDelta> execute(String datamart, Long deltaNum);
}
