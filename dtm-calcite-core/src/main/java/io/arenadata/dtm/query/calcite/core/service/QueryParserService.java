package io.arenadata.dtm.query.calcite.core.service;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Сервис парсинга запроса
 */
public interface QueryParserService {
    void parse(QueryParserRequest request, Handler<AsyncResult<QueryParserResponse>> asyncResultHandler);
}
