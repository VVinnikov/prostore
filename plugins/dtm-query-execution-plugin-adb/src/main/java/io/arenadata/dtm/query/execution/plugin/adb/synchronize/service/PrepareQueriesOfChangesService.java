package io.arenadata.dtm.query.execution.plugin.adb.synchronize.service;

import io.vertx.core.Future;

public interface PrepareQueriesOfChangesService {
    Future<PrepareRequestOfChangesResult> prepare(PrepareRequestOfChangesRequest request);
}
