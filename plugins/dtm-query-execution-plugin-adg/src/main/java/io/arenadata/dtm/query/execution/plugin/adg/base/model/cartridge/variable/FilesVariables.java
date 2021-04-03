package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.variable;

import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationFile;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Variable for sending configuration files
 */
@Data
@AllArgsConstructor
public class FilesVariables extends Variables {
    List<OperationFile> files;
}
