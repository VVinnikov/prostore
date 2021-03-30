package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import lombok.Data;

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
