package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.vertx.core.Future;

public interface StatusService {
    Future<StatusQueryResult> execute(String topic);
}
