package io.arenadata.dtm.query.execution.plugin.adp.init.service;

import io.arenadata.dtm.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpInitializationService")
public class AdpInitializationService implements PluginInitializationService {
    @Override
    public Future<Void> execute() {
        return Future.succeededFuture(); //todo ?
    }
}
