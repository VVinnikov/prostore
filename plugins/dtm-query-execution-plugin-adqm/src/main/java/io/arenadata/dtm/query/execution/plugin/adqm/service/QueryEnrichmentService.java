package io.arenadata.dtm.query.execution.plugin.adqm.service;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.vertx.core.Future;

/**
 * Query enrichment service
 */
public interface QueryEnrichmentService {

    Future<String> enrich(EnrichQueryRequest request);
}
