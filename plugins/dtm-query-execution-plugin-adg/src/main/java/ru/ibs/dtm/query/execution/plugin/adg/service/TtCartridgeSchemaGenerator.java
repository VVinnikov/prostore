package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

/**
 * Генератор схемы
 */
public interface TtCartridgeSchemaGenerator {
    void generate(DdlRequestContext context, OperationYaml yaml, Handler<AsyncResult<OperationYaml>> handler);
}
