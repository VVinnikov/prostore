package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Applying space schema and configuration into Tarantool Cartridge
 */
public interface AdgCartridgeProvider {
  Future<Void> apply(DdlRequestContext context);
}
