package io.arenadata.dtm.query.execution.core.metadata.service;

import io.vertx.core.Future;

/**
 * Service for executing ddl queries in plugins
 */
public interface MetadataExecutor<Request> {

    /**
     * Применить физическую модель на БД через плагин
     *
     * @param request dto-обертка для физическая модели
     */
    Future<Void> execute(Request request);
}
