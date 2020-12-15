package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Service for analyzing and executing query
 */
public interface QueryAnalyzer {

  /**
   *
   * @param inputQueryRequest       queryRequest
   * @param handler asyncHandler
   */
  void analyzeAndExecute(InputQueryRequest inputQueryRequest, AsyncHandler<QueryResult> handler);
}
