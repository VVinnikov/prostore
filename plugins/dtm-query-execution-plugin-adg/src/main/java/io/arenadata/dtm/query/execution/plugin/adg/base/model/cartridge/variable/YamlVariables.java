package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.variable;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Variable for sending YAML
 */
@Data
@AllArgsConstructor
public class YamlVariables extends Variables {
    String yaml;
}
