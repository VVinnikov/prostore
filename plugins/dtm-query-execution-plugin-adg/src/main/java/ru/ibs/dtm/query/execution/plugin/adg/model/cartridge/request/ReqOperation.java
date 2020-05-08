package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.variable.Variables;

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
