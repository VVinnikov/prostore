package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Service for target database definition
 */
public interface TargetDatabaseDefinitionService {

  /**
   * Get target source type
   *
   * @param request request
   * @return request with defined type
   */
  void getTargetSource(QuerySourceRequest request, Handler<AsyncResult<QuerySourceRequest>> handler);
}
