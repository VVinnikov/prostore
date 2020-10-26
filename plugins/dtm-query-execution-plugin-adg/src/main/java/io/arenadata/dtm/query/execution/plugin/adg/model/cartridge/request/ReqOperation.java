package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.variable.Variables;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Базовая операция
 *
 * @operationName название
 * @variables переменные
 * @query запрос
 */
@Data
@AllArgsConstructor
public abstract class ReqOperation {
  String operationName;
  Variables variables;
  String query;
}
