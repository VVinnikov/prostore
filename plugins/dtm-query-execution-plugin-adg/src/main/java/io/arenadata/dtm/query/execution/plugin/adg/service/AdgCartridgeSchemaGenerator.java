package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;

/**
 * Tarantool schema generator
 */
public interface AdgCartridgeSchemaGenerator {
    Future<OperationYaml> generate(DdlRequest request, OperationYaml yaml);
}
