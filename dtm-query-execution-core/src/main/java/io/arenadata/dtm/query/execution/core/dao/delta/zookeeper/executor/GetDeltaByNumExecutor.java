package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.vertx.core.Future;

public interface GetDeltaByNumExecutor extends DeltaDaoExecutor {
    Future<OkDelta> execute(String datamart, Long deltaNum);
}