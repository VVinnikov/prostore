package io.arenadata.dtm.query.execution.plugin.adb.service;

import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Query enrichment service
 */
public interface QueryEnrichmentService {

  Future<String> enrich(EnrichQueryRequest request);
}
