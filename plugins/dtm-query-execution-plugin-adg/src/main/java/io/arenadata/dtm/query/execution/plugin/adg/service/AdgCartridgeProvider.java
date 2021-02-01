package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;

/**
 * Applying space schema and configuration into Tarantool Cartridge
 */
public interface AdgCartridgeProvider {
  Future<Void> apply(DdlRequest request);
}
