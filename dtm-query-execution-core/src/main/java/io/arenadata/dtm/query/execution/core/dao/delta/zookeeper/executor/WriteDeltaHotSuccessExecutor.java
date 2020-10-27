package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;

import java.time.LocalDateTime;

public interface WriteDeltaHotSuccessExecutor extends DeltaDaoExecutor {
    Future<Long> execute(String datamart, LocalDateTime deltaHotDate);
}