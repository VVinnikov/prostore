package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response;

import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;

/**
 * Результат получения/изменения конфига
 *
 * @path название конфига при получении
 * @filename название конфига при изменении
 * @content строка конфига
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
