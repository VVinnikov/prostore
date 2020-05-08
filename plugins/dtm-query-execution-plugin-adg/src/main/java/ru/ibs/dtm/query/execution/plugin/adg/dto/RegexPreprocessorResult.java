package ru.ibs.dtm.query.execution.plugin.adg.dto;

import ru.ibs.dtm.common.reader.QueryRequest;

import java.util.Collections;
import java.util.Map;

public class RegexPreprocessorResult {
  private final QueryRequest originalQueryRequest;
  private String modifiedSql;
  private Map<String, String> systemTimesForTables;

  public RegexPreprocessorResult(QueryRequest originalQueryRequest) {
    this.originalQueryRequest = originalQueryRequest;
    modifiedSql = originalQueryRequest.getSql().replaceAll("\r\n", " ").replaceAll("\n", " ");
    systemTimesForTables = Collections.emptyMap();
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

  public void setSystemTimesForTables(Map<String, String> systemTimesForTables) {
    this.systemTimesForTables = systemTimesForTables;
  }

  public Map<String, String> getSystemTimesForTables() {
    return systemTimesForTables;
  }

  public QueryRequest getActualQueryRequest() {
    if (!isSqlModified())
      return originalQueryRequest;
    final QueryRequest copy = originalQueryRequest.copy();
    copy.setSql(modifiedSql);
    return copy;
  }
}
