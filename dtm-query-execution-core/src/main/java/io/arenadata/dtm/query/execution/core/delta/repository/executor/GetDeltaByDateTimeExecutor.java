package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.vertx.core.Future;

import java.time.LocalDateTime;

public interface GetDeltaByDateTimeExecutor extends DeltaDaoExecutor {
    Future<OkDelta> execute(String datamart, LocalDateTime dateTime);
}
