package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.verticle.TaskVerticleExecutor;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.plugin.core.PluginRegistry;
import org.springframework.plugin.core.config.EnablePluginRegistries;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.stream.Collectors;

@Service
@EnablePluginRegistries({DtmDataSourcePlugin.class})
public class DataSourcePluginServiceImpl implements DataSourcePluginService {

    private final PluginRegistry<DtmDataSourcePlugin, SourceType> pluginRegistry;
    private final TaskVerticleExecutor taskVerticleExecutor;
    private final Set<SourceType> sourceTypes;

    @Autowired
    public DataSourcePluginServiceImpl(
        @Qualifier("dtmDataSourcePluginRegistry") PluginRegistry<DtmDataSourcePlugin, SourceType> pluginRegistry,
        TaskVerticleExecutor taskVerticleExecutor) {
        this.taskVerticleExecutor = taskVerticleExecutor;
        this.pluginRegistry = pluginRegistry;
        this.sourceTypes = pluginRegistry.getPlugins().stream()
            .map(DtmDataSourcePlugin::getSourceType)
            .collect(Collectors.toSet());
    }

    @Override
    public Set<SourceType> getSourceTypes() {
        return sourceTypes;
    }

    @Override
    public void ddl(SourceType sourceType,
                    DdlRequestContext context,
                    Handler<AsyncResult<Void>> asyncResultHandler) {
        taskVerticleExecutor.execute(p -> getPlugin(sourceType).ddl(context, p), asyncResultHandler);
    }

    @Override
    public void llr(SourceType sourceType,
                    LlrRequestContext context,
                    Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        taskVerticleExecutor.execute(p -> getPlugin(sourceType).llr(context, p), asyncResultHandler);
    }

    @Override
    public void mppr(SourceType sourceType,
                     MpprRequestContext context,
                     Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        taskVerticleExecutor.execute(p -> getPlugin(sourceType).mppr(context, p), asyncResultHandler);
    }

    @Override
    public void mppw(SourceType sourceType,
                     MppwRequestContext context,
                     Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        taskVerticleExecutor.execute(p -> getPlugin(sourceType).mppw(context, p), asyncResultHandler);
    }

    @Override
    public void calcQueryCost(SourceType sourceType,
                              QueryCostRequestContext context,
                              Handler<AsyncResult<Integer>> asyncResultHandler) {
        taskVerticleExecutor.execute(p -> getPlugin(sourceType).calcQueryCost(context, p), asyncResultHandler);
    }

    @Override
    public void status(SourceType sourceType, StatusRequestContext context,
                       Handler<AsyncResult<StatusQueryResult>> asyncResultHandler) {
        taskVerticleExecutor.execute(p -> getPlugin(sourceType).status(context, p), asyncResultHandler);
    }

    @Override
    public void rollback(SourceType sourceType, RollbackRequestContext context,
                         Handler<AsyncResult<Void>> asyncResultHandler) {
        taskVerticleExecutor.execute(p -> getPlugin(sourceType).rollback(context, p), asyncResultHandler);
    }

    private DtmDataSourcePlugin getPlugin(SourceType sourceType) {
        return pluginRegistry.getRequiredPluginFor(sourceType);
    }
}
