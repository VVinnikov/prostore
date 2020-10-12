package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.InputQueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;

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
