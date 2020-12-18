package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.verticle.TaskVerticleExecutor;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.plugin.core.PluginRegistry;
import org.springframework.plugin.core.config.EnablePluginRegistries;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.function.Function;
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
    public Future<Void> ddl(SourceType sourceType, DdlRequestContext context) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.DDL,
                context.getMetrics(),
                plugin -> plugin.ddl(context));
    }

    @Override
    public Future<QueryResult> llr(SourceType sourceType, LlrRequestContext context) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.LLR,
                context.getMetrics(),
                plugin -> plugin.llr(context));
    }

    @Override
    public Future<QueryResult> mppr(SourceType sourceType, MpprRequestContext context) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.MPPR,
                context.getMetrics(),
                plugin -> plugin.mppr(context));
    }

    @Override
    public Future<QueryResult> mppw(SourceType sourceType, MppwRequestContext context) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.MPPW,
                context.getMetrics(),
                plugin -> plugin.mppw(context));
    }

    @Override
    public Future<Integer> calcQueryCost(SourceType sourceType, QueryCostRequestContext context) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.COST,
                context.getMetrics(),
                plugin -> plugin.calcQueryCost(context));
    }

    @Override
    public Future<StatusQueryResult> status(SourceType sourceType, StatusRequestContext context) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.STATUS,
                context.getMetrics(),
                plugin -> plugin.status(context));
    }

    @Override
    public Future<Void> rollback(SourceType sourceType, RollbackRequestContext context) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.ROLLBACK,
                context.getMetrics(),
                plugin -> plugin.rollback(context));
    }

    @Override
    public DtmDataSourcePlugin getPlugin(SourceType sourceType) {
        return pluginRegistry.getRequiredPluginFor(sourceType);
    }

    @Override
    public Set<String> getActiveCaches() {
        return activeCaches;
    }

    @Override
    public Future<Void> checkTable(SourceType sourceType, CheckContext context) {
        return check(new PluginParams(sourceType, context.getMetrics()),
                plugin -> plugin.checkTable(context));
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountParams params) {
        return check(params, plugin -> plugin.checkDataByCount(params));
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params) {
        return check(params, plugin -> plugin.checkDataByHashInt32(params));
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryParams params) {
        return executeWithMetrics(SqlProcessingType.TRUNCATE,
                params,
                plugin -> plugin.truncateHistory(params));
    }

    private <T> Future<T> check(PluginParams pluginParams,
                                Function<DtmDataSourcePlugin, Future<T>> func) {
        return executeWithMetrics(SqlProcessingType.CHECK, pluginParams, func);
    }

    private <T> Future<T> executeWithMetrics(SqlProcessingType sqlProcessingType,
                                             PluginParams pluginParams,
                                             Function<DtmDataSourcePlugin, Future<T>> func) {
        SourceType sourceType = pluginParams.getSourceType();
        RequestMetrics requestMetrics = pluginParams.getRequestMetrics();
        return executeWithMetrics(sourceType, sqlProcessingType, requestMetrics, func);
    }

    private <T> Future<T> executeWithMetrics(SourceType sourceType,
                                             SqlProcessingType sqlProcessingType,
                                             RequestMetrics requestMetrics,
                                             Function<DtmDataSourcePlugin, Future<T>> func) {
        return Future.future((Promise<T> promise) ->
                metricsService.sendMetrics(sourceType,
                        sqlProcessingType,
                        requestMetrics)
                        .compose(result -> execute(func.apply(getPlugin(sourceType))))
                        .onComplete(metricsService.sendMetrics(sourceType,
                                sqlProcessingType,
                                requestMetrics,
                                promise)));
    }

    private <T> Future<T> execute(Future<T> future) {
        return Future.future((Promise<T> promise) -> taskVerticleExecutor.execute(p -> future
                        .onSuccess(promise::complete)
                        .onFailure(p::fail),
                promise));
    }
}
