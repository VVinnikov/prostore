package ru.ibs.dtm.query.execution.plugin.api.dto;

import ru.ibs.dtm.common.reader.QueryRequest;

/**
 * Базовый dto для выполнения запросов через plugin-api
 */
public abstract class BaseRequest {

  /**
   * Оригинальный запрос
   */
  private QueryRequest queryRequest;

  public BaseRequest() {
  }

  public BaseRequest(QueryRequest queryRequest) {
    this.queryRequest = queryRequest;
  }

  public QueryRequest getQueryRequest() {
    return queryRequest;
  }

  public void setQueryRequest(QueryRequest queryRequest) {
    this.queryRequest = queryRequest;
  }

  @Override
  public String toString() {
    return "BaseRequest{" +
      "queryRequest=" + queryRequest +
      '}';
  }
}
