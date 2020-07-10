package ru.ibs.dtm.query.calcite.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.dto.QueryParserRequest;
import ru.ibs.dtm.common.dto.QueryParserResponse;

/**
 * Сервис парсинга запроса
 */
public interface QueryParserService {
    void parse(QueryParserRequest request, Handler<AsyncResult<QueryParserResponse>> asyncResultHandler);
}
