package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Файл конфигурации:
 *
 * @filename название
 * @content конфигурация в виде строки
 */
@Data
@AllArgsConstructor
public class OperationFile {
  String filename;
  String content;
}
