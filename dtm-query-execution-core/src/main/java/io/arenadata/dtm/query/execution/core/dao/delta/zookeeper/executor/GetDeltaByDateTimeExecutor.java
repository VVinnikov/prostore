package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.vertx.core.Future;

import java.time.LocalDateTime;

public interface GetDeltaByDateTimeExecutor extends DeltaDaoExecutor {
    Future<OkDelta> execute(String datamart, LocalDateTime dateTime);
}
