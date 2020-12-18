package io.arenadata.dtm.query.execution.core.service.rollback;

import io.vertx.core.Future;

public interface RestoreStateService {

    Future<Void> restoreState();
}
