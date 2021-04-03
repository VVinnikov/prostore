package io.arenadata.dtm.query.execution.plugin.adqm.enrichment.service;

import io.arenadata.dtm.query.execution.plugin.adqm.enrichment.dto.EnrichQueryRequest;
import io.vertx.core.Future;

/**
 * Query enrichment service
 */
public interface QueryEnrichmentService {

    Future<String> enrich(EnrichQueryRequest request);
}
