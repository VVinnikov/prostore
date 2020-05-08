package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;

/**
 * Сервис обогащения SQL
 */
public interface QueryEnrichmentService {

  void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> handler);
}
