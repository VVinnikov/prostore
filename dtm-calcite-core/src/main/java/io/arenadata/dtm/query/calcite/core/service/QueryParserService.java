package io.arenadata.dtm.query.calcite.core.service;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Query parsing service
 */
public interface QueryParserService {
    void parse(QueryParserRequest request, AsyncHandler<QueryParserResponse> handler);
}
