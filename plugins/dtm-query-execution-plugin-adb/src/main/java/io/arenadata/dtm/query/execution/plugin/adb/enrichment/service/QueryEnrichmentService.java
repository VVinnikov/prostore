package io.arenadata.dtm.query.execution.plugin.adb.enrichment.service;

import io.arenadata.dtm.query.execution.plugin.adb.enrichment.dto.EnrichQueryRequest;
import io.vertx.core.Future;

/**
 * Query enrichment service
 */
public interface QueryEnrichmentService {

  Future<String> enrich(EnrichQueryRequest request);
}
