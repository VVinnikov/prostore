package io.arenadata.dtm.query.execution.plugin.api.service;

import io.vertx.core.Future;

public interface PluginInitializationService {

    Future<Void> execute();
}
