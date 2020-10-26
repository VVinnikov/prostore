package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Применение схемы и конфигурации в рамках Tarantool Cartridge
 */
public interface TtCartridgeProvider {
  void apply(DdlRequestContext context, Handler<AsyncResult<Void>> handler);
}
