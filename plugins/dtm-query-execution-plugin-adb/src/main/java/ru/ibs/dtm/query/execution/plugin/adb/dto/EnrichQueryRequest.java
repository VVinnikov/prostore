package ru.ibs.dtm.query.execution.plugin.adb.dto;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.reader.QueryRequest;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrichQueryRequest {
  private QueryRequest queryRequest;
  private JsonObject schema;
  public static EnrichQueryRequest generate(QueryRequest queryRequest, JsonObject schema) {
    return new EnrichQueryRequest(queryRequest,schema);
  }
}
