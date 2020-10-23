package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Генератор схемы
 */
public interface TtCartridgeSchemaGenerator {
    void generate(DdlRequestContext context, OperationYaml yaml, Handler<AsyncResult<OperationYaml>> handler);
}
