package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.variable.Variables;

/**
 * Получить файлы конфигурации
 */
public class GetFilesOperation extends ReqOperation {

  public GetFilesOperation() {
    super("configFiles", new Variables() {},
      "query configFiles { cluster { config { path: filename content } }}");
  }
}
