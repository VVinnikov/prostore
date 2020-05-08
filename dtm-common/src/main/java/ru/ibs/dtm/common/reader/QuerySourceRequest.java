package ru.ibs.dtm.common.reader;

/*Дто с модифицированным sql запросом, из которого извлечен хинт*/
public class QuerySourceRequest {
  private QueryRequest queryRequest;
  private SourceType sourceType;

  public QuerySourceRequest() {
  }

  public QuerySourceRequest(QueryRequest queryRequest, SourceType sourceType) {
    this.queryRequest = queryRequest;
    this.sourceType = sourceType;
  }

  public QueryRequest getQueryRequest() {
    return queryRequest;
  }

  public void setQueryRequest(QueryRequest queryRequest) {
    this.queryRequest = queryRequest;
  }

  public SourceType getSourceType() {
    return sourceType;
  }

  public void setSourceType(SourceType sourceType) {
    this.sourceType = sourceType;
  }
}
