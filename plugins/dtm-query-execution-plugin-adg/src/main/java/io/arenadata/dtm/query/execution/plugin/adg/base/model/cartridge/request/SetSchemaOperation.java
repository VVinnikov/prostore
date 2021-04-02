package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.variable.YamlVariables;

/**
 * Set schema
 */
public class SetSchemaOperation extends ReqOperation {

    public SetSchemaOperation(String yaml) {
        super("set_schema", new YamlVariables(yaml),
                "mutation set_schema($yaml: String!) {\n" +
                        " cluster { schema(as_yaml: $yaml) { as_yaml }}\n" +
                        "}");
    }
}
