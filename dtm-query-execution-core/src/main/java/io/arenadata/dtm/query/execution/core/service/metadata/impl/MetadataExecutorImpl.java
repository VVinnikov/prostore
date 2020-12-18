package io.arenadata.dtm.query.execution.core.service.metadata.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Component
public class MetadataExecutorImpl implements MetadataExecutor<DdlRequestContext> {

    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public MetadataExecutorImpl(DataSourcePluginService dataSourcePluginService) {
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        return Future.future(promise -> {
            List<Future> futures = new ArrayList<>();
            Set<SourceType> destination = Optional.ofNullable(context.getRequest().getEntity())
                    .map(Entity::getDestination)
                    .filter(set -> !set.isEmpty())
                    .orElse(dataSourcePluginService.getSourceTypes());
            destination.forEach(sourceType ->
                    futures.add(dataSourcePluginService.ddl(
                            sourceType,
                            context)
                    ));
            CompositeFuture.join(futures).setHandler(ar -> {
                if (ar.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }
}
