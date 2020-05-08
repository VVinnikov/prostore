package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.variable.Variables;

/**
 * Получить текущую схему
 */
public class GetSchemaOperation extends ReqOperation {

  public GetSchemaOperation() {
    super("get_schema", new Variables() {},
      "query get_schema { cluster { schema { as_yaml } } }");
  }
}
