package io.arenadata.dtm.query.execution.core.service.init;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.stream.Collectors;

@Service("coreInitializationService")
@Slf4j
public class CoreInitializationService implements PluginInitializationService {

    private final DataSourcePluginService sourcePluginService;

    @Autowired
    public CoreInitializationService(DataSourcePluginService sourcePluginService) {
        this.sourcePluginService = sourcePluginService;
    }

    @Override
    public Future<Void> execute() {
        return Future.future(promise -> {
            Set<SourceType> sourceTypes = sourcePluginService.getSourceTypes();
            CompositeFuture.join(sourceTypes.stream()
                    .map(sourcePluginService::initialize)
                    .collect(Collectors.toList()))
                    .onSuccess(s -> {
                        log.info("Plugins: {} initialized successfully", sourceTypes);
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }
}
