package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;

/**
 * Сервис исполнения запроса для LLR.
 */
public interface LlrService {

  void executeQuery(LlrRequest request, Handler<AsyncResult<QueryResult>> handler);
}
