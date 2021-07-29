package io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.vertx.core.Future;

/**
 * Query enrichment service
 */
public interface QueryEnrichmentService {

    Future<String> enrich(EnrichQueryRequest request, QueryParserResponse parserResponse);
}
