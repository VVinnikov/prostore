package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;
import ru.ibs.dtm.query.execution.core.dto.delta.HotDelta;

public interface GetDeltaHotExecutor extends DeltaDaoExecutor {
    Future<HotDelta> execute(String datamart);
}
