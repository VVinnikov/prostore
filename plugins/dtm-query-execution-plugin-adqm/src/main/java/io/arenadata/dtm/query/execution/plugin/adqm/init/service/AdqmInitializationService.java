package io.arenadata.dtm.query.execution.plugin.adqm.init.service;

import io.arenadata.dtm.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adqmInitializationService")
public class AdqmInitializationService implements PluginInitializationService {

    @Override
    public Future<Void> execute() {
        return Future.succeededFuture();
    }
}
