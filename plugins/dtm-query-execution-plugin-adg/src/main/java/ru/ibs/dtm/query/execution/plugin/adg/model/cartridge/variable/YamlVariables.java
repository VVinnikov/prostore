package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.variable;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Переменная для передачи YAML
 */
@Data
@AllArgsConstructor
public class YamlVariables extends Variables {
  String yaml;
}
