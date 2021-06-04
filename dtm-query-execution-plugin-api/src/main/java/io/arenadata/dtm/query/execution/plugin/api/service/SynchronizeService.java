package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;

public interface SynchronizeService {
    Future<Long> execute(SynchronizeRequest request);
}
