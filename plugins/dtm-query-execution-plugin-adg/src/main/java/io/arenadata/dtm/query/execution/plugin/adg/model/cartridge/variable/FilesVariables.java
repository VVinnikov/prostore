package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.variable;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Переменная для передачи файлов конфигурации
 */
@Data
@AllArgsConstructor
public class FilesVariables extends Variables {
  List<OperationFile> files;
}
