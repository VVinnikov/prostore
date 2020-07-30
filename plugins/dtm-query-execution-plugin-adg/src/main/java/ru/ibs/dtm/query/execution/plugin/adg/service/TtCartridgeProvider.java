package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

/**
 * Применение схемы и конфигурации в рамках Tarantool Cartridge
 */
public interface TtCartridgeProvider {
  void apply(DdlRequestContext context, Handler<AsyncResult<Void>> handler);
}
