package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
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
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.plugin.core.PluginRegistry;
import org.springframework.plugin.core.config.EnablePluginRegistries;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@EnablePluginRegistries({DtmDataSourcePlugin.class})
public class DataSourcePluginServiceImpl implements DataSourcePluginService {

    private final PluginRegistry<DtmDataSourcePlugin, SourceType> pluginRegistry;
    private final TaskVerticleExecutor taskVerticleExecutor;
    private final Set<SourceType> sourceTypes;
    private final Set<String> activeCaches;
    private final MetricsService<RequestMetrics> metricsService;

    @Autowired
    public DataSourcePluginServiceImpl(
            PluginRegistry<DtmDataSourcePlugin, SourceType> pluginRegistry,
            TaskVerticleExecutor taskVerticleExecutor,
            @Qualifier("coreMetricsService") MetricsService<RequestMetrics> metricsService) {
        this.taskVerticleExecutor = taskVerticleExecutor;
        this.pluginRegistry = pluginRegistry;
        this.sourceTypes = pluginRegistry.getPlugins().stream()
                .map(DtmDataSourcePlugin::getSourceType)
                .collect(Collectors.toSet());
        this.activeCaches = pluginRegistry.getPlugins().stream()
                .flatMap(plugin -> plugin.getActiveCaches().stream())
                .collect(Collectors.toSet());
        this.metricsService = metricsService;
        log.info("Active Plugins: {}", sourceTypes.toString());
    }

    @Override
    public Set<SourceType> getSourceTypes() {
        return sourceTypes;
    }

    @Override
    public void ddl(SourceType sourceType,
                    DdlRequestContext context,
                    Handler<AsyncResult<Void>> asyncResultHandler) {
        metricsService.sendMetrics(sourceType,
                SqlProcessingType.DDL,
                context.getMetrics())
                .onSuccess(ar -> {
                    taskVerticleExecutor.execute(((Promise<Void> p) -> getPlugin(sourceType).ddl(context, p)),
                            metricsService.sendMetrics(sourceType,
                                    SqlProcessingType.DDL,
                                    context.getMetrics(),
                                    asyncResultHandler));
                })
                .onFailure(fail -> asyncResultHandler.handle(Future.failedFuture(fail)));
    }

    @Override
    public void llr(SourceType sourceType,
                    LlrRequestContext context,
                    Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        metricsService.sendMetrics(sourceType,
                SqlProcessingType.LLR,
                context.getMetrics())
                .onSuccess(ar -> {
                    taskVerticleExecutor.execute(p -> getPlugin(sourceType).llr(context, p),
                            metricsService.sendMetrics(sourceType,
                                    SqlProcessingType.LLR,
                                    context.getMetrics(),
                                    asyncResultHandler));
                })
                .onFailure(fail -> asyncResultHandler.handle(Future.failedFuture(fail)));
    }

    @Override
    public void mppr(SourceType sourceType,
                     MpprRequestContext context,
                     Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        metricsService.sendMetrics(sourceType,
                SqlProcessingType.MPPR,
                context.getMetrics())
                .onSuccess(ar -> {
                    taskVerticleExecutor.execute(p -> getPlugin(sourceType).mppr(context, p),
                            metricsService.sendMetrics(sourceType,
                                    SqlProcessingType.MPPR,
                                    context.getMetrics(),
                                    asyncResultHandler));
                })
                .onFailure(fail -> asyncResultHandler.handle(Future.failedFuture(fail)));
    }

    @Override
    public void mppw(SourceType sourceType,
                     MppwRequestContext context,
                     Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        metricsService.sendMetrics(sourceType,
                SqlProcessingType.MPPW,
                context.getMetrics())
                .onSuccess(ar -> {
                    taskVerticleExecutor.execute(p -> getPlugin(sourceType).mppw(context, p),
                            metricsService.sendMetrics(sourceType,
                                    SqlProcessingType.MPPW,
                                    context.getMetrics(),
                                    asyncResultHandler));
                })
                .onFailure(fail -> asyncResultHandler.handle(Future.failedFuture(fail)));
    }

    @Override
    public void calcQueryCost(SourceType sourceType,
                              QueryCostRequestContext context,
                              Handler<AsyncResult<Integer>> asyncResultHandler) {
        metricsService.sendMetrics(sourceType,
                SqlProcessingType.MPPW,
                context.getMetrics())
                .onSuccess(ar -> {
                    taskVerticleExecutor.execute(p -> getPlugin(sourceType).calcQueryCost(context, p),
                            metricsService.sendMetrics(sourceType,
                                    SqlProcessingType.COST,
                                    context.getMetrics(),
                                    asyncResultHandler));
                })
                .onFailure(fail -> asyncResultHandler.handle(Future.failedFuture(fail)));
    }

    @Override
    public void status(SourceType sourceType, StatusRequestContext context,
                       Handler<AsyncResult<StatusQueryResult>> asyncResultHandler) {
        metricsService.sendMetrics(sourceType,
                SqlProcessingType.STATUS,
                context.getMetrics())
                .onSuccess(ar -> {
                    taskVerticleExecutor.execute(p -> getPlugin(sourceType).status(context, p),
                            metricsService.sendMetrics(sourceType,
                                    SqlProcessingType.STATUS,
                                    context.getMetrics(),
                                    asyncResultHandler));
                })
                .onFailure(fail -> asyncResultHandler.handle(Future.failedFuture(fail)));
    }

    @Override
    public void rollback(SourceType sourceType, RollbackRequestContext context,
                         Handler<AsyncResult<Void>> asyncResultHandler) {
        metricsService.sendMetrics(sourceType,
                SqlProcessingType.ROLLBACK,
                context.getMetrics())
                .onSuccess(ar -> {
                    taskVerticleExecutor.execute(p -> getPlugin(sourceType).rollback(context, p),
                            metricsService.sendMetrics(sourceType,
                                    SqlProcessingType.ROLLBACK,
                                    context.getMetrics(),
                                    asyncResultHandler));
                })
                .onFailure(fail -> asyncResultHandler.handle(Future.failedFuture(fail)));
    }

    @Override
    public DtmDataSourcePlugin getPlugin(SourceType sourceType) {
        return pluginRegistry.getRequiredPluginFor(sourceType);
    }

    @Override
    public Set<String> getActiveCaches() {
        return activeCaches;
    }
}
