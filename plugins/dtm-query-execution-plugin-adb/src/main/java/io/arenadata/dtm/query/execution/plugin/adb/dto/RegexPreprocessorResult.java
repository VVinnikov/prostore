package io.arenadata.dtm.query.execution.plugin.adb.dto;

import io.arenadata.dtm.common.reader.QueryRequest;

public class RegexPreprocessorResult {
  private final QueryRequest originalQueryRequest;
  private String modifiedSql;

  private String systemTimeAsOf;

  public RegexPreprocessorResult(QueryRequest originalQueryRequest) {
    this.originalQueryRequest = originalQueryRequest;
    modifiedSql = originalQueryRequest.getSql().replaceAll("\r\n", " ").replaceAll("\n", " ");
  }

  public boolean isSqlModified() {
    return !originalQueryRequest.getSql().equals(modifiedSql);
  }

  public QueryRequest getOriginalQueryRequest() {
    return originalQueryRequest;
  }

  public String getModifiedSql() {
    return modifiedSql;
  }

  public void setModifiedSql(String modifiedSql) {
    this.modifiedSql = modifiedSql;
  }

  public String getSystemTimeAsOf() {
    return systemTimeAsOf;
  }

  public void setSystemTimeAsOf(String systemTimeAsOf) {
    this.systemTimeAsOf = systemTimeAsOf;
  }

  public QueryRequest getActualQueryRequest() {
    if (!isSqlModified())
      return originalQueryRequest;
    final QueryRequest copy = originalQueryRequest.copy();
    copy.setSql(modifiedSql);
    return copy;
  }
}
