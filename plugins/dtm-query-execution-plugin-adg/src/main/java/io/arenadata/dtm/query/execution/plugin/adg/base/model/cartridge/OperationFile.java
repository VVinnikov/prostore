package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Configuration file:
 *
 * @filename name
 * @content configuration as string
 */
@Data
@AllArgsConstructor
public class OperationFile {
    String filename;
    String content;
}
