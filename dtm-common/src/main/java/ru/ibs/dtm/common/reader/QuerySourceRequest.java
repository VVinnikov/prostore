package ru.ibs.dtm.common.reader;

import io.vertx.core.json.JsonObject;
import lombok.*;

/*Дто с модифицированным sql запросом, из которого извлечен хинт*/
@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class QuerySourceRequest {
  @NonNull
  private QueryRequest queryRequest;
  private JsonObject logicalSchema;
  @NonNull
  private SourceType sourceType;
}
