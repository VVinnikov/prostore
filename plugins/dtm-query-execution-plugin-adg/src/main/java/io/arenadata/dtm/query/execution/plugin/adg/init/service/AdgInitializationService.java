package io.arenadata.dtm.query.execution.plugin.adg.init.service;

import io.arenadata.dtm.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adgInitializationService")
public class AdgInitializationService implements PluginInitializationService {

    @Override
    public Future<Void> execute() {
        return Future.succeededFuture();
    }
}
