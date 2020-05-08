package ru.ibs.dtm.query.execution.plugin.api.dto;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.common.reader.QueryRequest;

/**
 * dto для ввполнения llr
 */
public class LlrRequest extends BaseRequest{

  /**
   * Логическая схема
   */
  private JsonObject schema;

  public LlrRequest() {
  }

  public LlrRequest(QueryRequest queryRequest, JsonObject schema) {
    super(queryRequest);
    this.schema = schema;
  }

  public JsonObject getSchema() {
    return schema;
  }

  public void setSchema(JsonObject schema) {
    this.schema = schema;
  }

  @Override
  public String toString() {
    return "LlrRequest{" +
      super.toString() +
      ", schema=" + schema +
      '}';
  }
}
