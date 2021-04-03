package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response;

import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationFile;
import lombok.Data;

/**
 * Result of getting/changing config
 *
 * @path config name upon receipt
 * @filename config name when changed
 * @content config string
 */
@Data
public class ResConfig {
    String path;
    String filename;
    String content;

    public OperationFile toOperationFile() {
        return new OperationFile(path, content);
    }
}
