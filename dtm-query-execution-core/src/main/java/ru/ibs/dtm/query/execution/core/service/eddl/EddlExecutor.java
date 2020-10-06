package ru.ibs.dtm.query.execution.core.service.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlAction;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;

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
