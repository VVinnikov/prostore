package io.arenadata.dtm.query.execution.core.service;

import io.vertx.core.Future;

public interface RestoreStateService {

    Future<Void> restoreState();
}
