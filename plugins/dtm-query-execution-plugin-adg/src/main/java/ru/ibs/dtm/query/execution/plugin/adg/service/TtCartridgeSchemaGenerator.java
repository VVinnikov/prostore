package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;

import java.util.List;

/**
 * Генератор схемы
 */
public interface TtCartridgeSchemaGenerator {
  void generate(ClassTable classTable, OperationYaml yaml, Handler<AsyncResult<OperationYaml>> handler);
  void setConfig(ClassTable classTable, List<OperationFile> files, Handler<AsyncResult<List<OperationFile>>> handler);
  void deleteConfig(ClassTable classTable, List<OperationFile> files, Handler<AsyncResult<List<OperationFile>>> handler);
}
