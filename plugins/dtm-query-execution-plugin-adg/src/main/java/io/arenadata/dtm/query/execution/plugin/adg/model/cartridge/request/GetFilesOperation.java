package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.variable.Variables;

/**
 * Получить файлы конфигурации
 */
public class GetFilesOperation extends ReqOperation {

  public GetFilesOperation() {
    super("configFiles", new Variables() {},
      "query configFiles { cluster { config { path: filename content } }}");
  }
}
