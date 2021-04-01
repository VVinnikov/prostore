package io.arenadata.dtm.query.execution.core.query.service;

import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.vertx.core.Future;

/**
 * Service for analyzing and executing query
 */
public interface QueryAnalyzer {

  /**
   *  @param inputQueryRequest       queryRequest
   * @return query result
   */
  Future<QueryResult> analyzeAndExecute(InputQueryRequest inputQueryRequest);
}
