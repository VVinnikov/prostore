package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor;

import io.vertx.core.Future;

import java.time.LocalDateTime;

public interface WriteDeltaHotSuccessExecutor extends DeltaDaoExecutor {
    Future<Void> execute(String datamart, LocalDateTime deltaHotDate);
}
