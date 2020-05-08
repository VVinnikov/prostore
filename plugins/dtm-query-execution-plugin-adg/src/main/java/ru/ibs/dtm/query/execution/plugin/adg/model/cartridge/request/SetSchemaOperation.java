package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.variable.YamlVariables;

/**
 * Установить схему
 */
public class SetSchemaOperation extends ReqOperation {

  public SetSchemaOperation(String yaml) {
    super("set_schema", new YamlVariables(yaml),
      "mutation set_schema($yaml: String!) {\n" +
      " cluster { schema(as_yaml: $yaml) { as_yaml }}\n" +
      "}");
  }
}
