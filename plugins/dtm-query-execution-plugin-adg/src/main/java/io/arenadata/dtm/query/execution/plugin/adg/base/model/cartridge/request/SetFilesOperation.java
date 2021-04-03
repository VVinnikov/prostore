package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationFile;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.variable.FilesVariables;

import java.util.List;

/**
 * Set configuration files
 */
public class SetFilesOperation extends ReqOperation {

    public SetFilesOperation(List<OperationFile> files) {
        super("set_files", new FilesVariables(files),
                "mutation set_files($files: [ConfigSectionInput!]) { cluster { " +
                        "config(sections: $files) { filename content } } } ");
    }
}
