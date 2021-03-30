package io.arenadata.dtm.query.execution.core.service.eddl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlAction;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.vertx.core.Future;

/**
 * EDDL query executor
 */
public interface EddlExecutor {

  /**
   * <p>Execute EDDL query</p>
   *  @param query              query
   * @return
   */
  Future<QueryResult> execute(EddlQuery query);

  /**
   * Get query type
   *
   * @return query type
   */
  EddlAction getAction();
}
