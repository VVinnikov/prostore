package ru.ibs.dtm.query.execution.core.service.metadata.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.ArrayList;
import java.util.List;

@Component
public class MetadataExecutorImpl implements MetadataExecutor<DdlRequestContext> {

    private DataSourcePluginService dataSourcePluginService;

    @Autowired
    public MetadataExecutorImpl(DataSourcePluginService dataSourcePluginService) {
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public void execute(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
        List<Future> futures = new ArrayList<>();
        dataSourcePluginService.getSourceTypes().forEach(sourceType ->
                futures.add(Future.future(p -> dataSourcePluginService.ddl(
                        sourceType,
                        context,
                        ar -> {
                            if (ar.succeeded()) {
                                p.complete();
                            } else {
                                p.fail(ar.cause());
                            }
                        }))));
        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
