package io.arenadata.dtm.query.execution.core.delta.repository.executor;

import io.vertx.core.Future;

import java.time.LocalDateTime;

public interface WriteDeltaHotSuccessExecutor extends DeltaDaoExecutor {
    Future<LocalDateTime> execute(String datamart, LocalDateTime deltaHotDate);
}
