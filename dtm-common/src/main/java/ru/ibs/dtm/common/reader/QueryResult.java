package ru.ibs.dtm.common.reader;

import io.vertx.core.json.JsonArray;

import java.util.Objects;
import java.util.UUID;

/**
 * Результат выполнения запроса.
 */
public class QueryResult {
  private UUID requestId;
  private JsonArray result;

  public static QueryResult emptyResult() {
    return new QueryResult(null, new JsonArray());
  }

  public QueryResult(UUID requestId, JsonArray result) {
    this.requestId = requestId;
    this.result = result;
  }

  public QueryResult() {
  }

  public boolean isEmpty() {
    return result == null || result.isEmpty();
  }

  public UUID getRequestId() {
    return requestId;
  }

  public void setRequestId(UUID requestId) {
    this.requestId = requestId;
  }

  public JsonArray getResult() {
    return result;
  }

  public void setResult(JsonArray result) {
    this.result = result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryResult result1 = (QueryResult) o;
    return Objects.equals(getRequestId(), result1.getRequestId()) &&
      Objects.equals(getResult(), result1.getResult());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRequestId(), getResult());
  }
}
