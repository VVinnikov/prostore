package io.arenadata.dtm.query.execution.plugin.adb.base.service.enrichment;

import io.arenadata.dtm.query.execution.plugin.adb.base.dto.EnrichQueryRequest;
import io.vertx.core.Future;

/**
 * Query enrichment service
 */
public interface QueryEnrichmentService {

  Future<String> enrich(EnrichQueryRequest request);
}
