package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Сервис обогащения SQL
 */
public interface QueryEnrichmentService {

  void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> handler);
}
