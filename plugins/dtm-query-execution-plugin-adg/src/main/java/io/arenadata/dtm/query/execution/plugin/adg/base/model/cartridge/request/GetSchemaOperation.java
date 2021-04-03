package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.variable.Variables;

/**
 * Get current schema
 */
public class GetSchemaOperation extends ReqOperation {

    public GetSchemaOperation() {
        super("get_schema", new Variables() {
                },
                "query get_schema { cluster { schema { as_yaml } } }");
    }
}
