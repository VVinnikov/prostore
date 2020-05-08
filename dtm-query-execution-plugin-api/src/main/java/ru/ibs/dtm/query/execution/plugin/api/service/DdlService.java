package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;

/**
 * Сервис исполнения запроса для DDL.
 */
public interface DdlService {

  void execute(DdlRequest request, Handler<AsyncResult<Void>> handler);
}
