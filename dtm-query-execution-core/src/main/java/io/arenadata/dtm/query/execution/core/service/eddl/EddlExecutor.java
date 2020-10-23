package io.arenadata.dtm.query.execution.core.service.eddl;

import io.arenadata.dtm.query.execution.core.dto.eddl.EddlAction;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * EDDL query executor
 */
public interface EddlExecutor {

  /**
   * <p>Execute EDDL query</p>
   *
   * @param query              query
   * @param asyncResultHandler async result handler
   */
  void execute(EddlQuery query, Handler<AsyncResult<Void>> asyncResultHandler);

  /**
   * Get query type
   *
   * @return query type
   */
  EddlAction getAction();
}
