package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Tarantool schema generator
 */
public interface AdgCartridgeSchemaGenerator {
    Future<OperationYaml> generate(DdlRequestContext context, OperationYaml yaml);
}
