package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ClassTable;

/**
 * Применение схемы и конфигурации в рамках Tarantool Cartridge
 */
public interface TtCartridgeProvider {
  void apply(ClassTable classTable, Handler<AsyncResult<Void>> handler);
  void delete(ClassTable classTable, Handler<AsyncResult<Void>> handler);

}
