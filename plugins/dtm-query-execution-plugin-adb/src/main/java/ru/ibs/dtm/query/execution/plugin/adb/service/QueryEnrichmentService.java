package ru.ibs.dtm.query.execution.plugin.adb.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;

/**
 * Сервис обогащения SQL
 */
public interface QueryEnrichmentService {

  void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> handler);
}
