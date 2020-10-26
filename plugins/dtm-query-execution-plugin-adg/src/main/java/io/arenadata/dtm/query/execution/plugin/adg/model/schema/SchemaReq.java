package io.arenadata.dtm.query.execution.plugin.adg.model.schema;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Запрос на регистрацию схемы
 *
 * @schema схема
 */
@Data
@AllArgsConstructor
public class SchemaReq {
  String schema;
}
