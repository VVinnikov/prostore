package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.variable.Variables;

/**
 * Get configuration files
 */
public class GetFilesOperation extends ReqOperation {

    public GetFilesOperation() {
        super("configFiles", new Variables() {
                },
                "query configFiles { cluster { config { path: filename content } }}");
    }
}
