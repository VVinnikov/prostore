package io.arenadata.dtm.query.execution.plugin.adg.enrichment.service;

import io.arenadata.dtm.query.execution.plugin.adg.enrichment.dto.EnrichQueryRequest;
import io.vertx.core.Future;

/**
 * Query enrichment service
 */
public interface QueryEnrichmentService {

    Future<String> enrich(EnrichQueryRequest request);
}
