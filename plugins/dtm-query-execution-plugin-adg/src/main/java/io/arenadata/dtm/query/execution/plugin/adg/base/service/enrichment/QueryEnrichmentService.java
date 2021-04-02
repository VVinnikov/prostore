package io.arenadata.dtm.query.execution.plugin.adg.base.service.enrichment;

import io.arenadata.dtm.query.execution.plugin.adg.base.dto.EnrichQueryRequest;
import io.vertx.core.Future;

/**
 * Query enrichment service
 */
public interface QueryEnrichmentService {

    Future<String> enrich(EnrichQueryRequest request);
}
