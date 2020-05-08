package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.variable;

import lombok.AllArgsConstructor;
import lombok.Data;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;

import java.util.List;

/**
 * Переменная для передачи файлов конфигурации
 */
@Data
@AllArgsConstructor
public class FilesVariables extends Variables {
  List<OperationFile> files;
}
