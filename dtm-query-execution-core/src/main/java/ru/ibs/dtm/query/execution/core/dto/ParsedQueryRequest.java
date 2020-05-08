package ru.ibs.dtm.query.execution.core.dto;

import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.service.SqlProcessingType;

/**
 * dto для передачи информации диспетчеру
 */
public class ParsedQueryRequest {

  private QueryRequest queryRequest;
  private SqlProcessingType processingType;

  public ParsedQueryRequest() {
  }

  public ParsedQueryRequest(QueryRequest queryRequest, SqlProcessingType processingType) {
    this.queryRequest = queryRequest;
    this.processingType = processingType;
  }

  public QueryRequest getQueryRequest() {
    return queryRequest;
  }

  public void setQueryRequest(QueryRequest queryRequest) {
    this.queryRequest = queryRequest;
  }

  public SqlProcessingType getProcessingType() {
    return processingType;
  }

  public void setProcessingType(SqlProcessingType processingType) {
    this.processingType = processingType;
  }
}
