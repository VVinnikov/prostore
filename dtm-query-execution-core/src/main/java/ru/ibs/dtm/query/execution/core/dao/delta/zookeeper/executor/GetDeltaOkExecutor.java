package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;
import ru.ibs.dtm.query.execution.core.dto.delta.OkDelta;

public interface GetDeltaOkExecutor extends DeltaDaoExecutor {
    Future<OkDelta> execute(String datamart);
}
