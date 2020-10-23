package io.arenadata.dtm.query.execution.core.service;

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
   * @param asyncResultHandler asyncHandler
   */
  void analyzeAndExecute(InputQueryRequest inputQueryRequest, Handler<AsyncResult<QueryResult>> asyncResultHandler);
}
