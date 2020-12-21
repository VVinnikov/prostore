package io.arenadata.dtm.query.calcite.core.service;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.vertx.core.Future;

/**
 * Query parsing service
 */
public interface QueryParserService {
    Future<QueryParserResponse> parse(QueryParserRequest request);
}
