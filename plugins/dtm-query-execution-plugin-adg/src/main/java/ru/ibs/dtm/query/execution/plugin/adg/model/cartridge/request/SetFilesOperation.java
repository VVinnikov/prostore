package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.variable.FilesVariables;

import java.util.List;

/**
 * Установить файлы конфигурации
 */
public class SetFilesOperation extends ReqOperation {

  public SetFilesOperation(List<OperationFile> files) {
    super("set_files", new FilesVariables(files),
      "mutation set_files($files: [ConfigSectionInput!]) { cluster { " +
        "config(sections: $files) { filename content } } } ");
  }
}
