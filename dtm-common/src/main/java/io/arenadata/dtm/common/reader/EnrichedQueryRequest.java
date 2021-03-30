package io.arenadata.dtm.common.reader;

/**
 * Запрос с обогащенным запросом DML
 */
public class EnrichedQueryRequest {

  /**
   * Исходный запрос
   */
  private QueryRequest queryRequest;

  /**
   * Обогащенный запрос
   */
  private String enrichSql;

  public EnrichedQueryRequest() {
  }

  public EnrichedQueryRequest(QueryRequest queryRequest, String enrichSql) {
    this.queryRequest = queryRequest;
    this.enrichSql = enrichSql;
  }

  public QueryRequest getQueryRequest() {
    return queryRequest;
  }

  public void setQueryRequest(QueryRequest queryRequest) {
    this.queryRequest = queryRequest;
  }

  public String getEnrichSql() {
    return enrichSql;
  }

  public void setEnrichSql(String enrichSql) {
    this.enrichSql = enrichSql;
  }
}
