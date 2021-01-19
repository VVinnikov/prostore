package io.arenadata.dtm.query.execution.core.service.datasource.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.verticle.TaskVerticleExecutor;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprPluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
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
    public Future<Void> ddl(SourceType sourceType, RequestMetrics metrics, DdlRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.DDL,
                metrics,
                plugin -> plugin.ddl(request));
    }

    @Override
    public Future<QueryResult> llr(SourceType sourceType, RequestMetrics metrics, LlrRequest llrRequest) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.LLR,
                metrics,
                plugin -> plugin.llr(llrRequest));
    }

    @Override
    public Future<QueryResult> mppr(SourceType sourceType, MpprPluginRequest request) {
        return getPlugin(sourceType).mppr(request);
    }

    @Override
    public Future<QueryResult> mppw(SourceType sourceType, MppwPluginRequest request) {
        return getPlugin(sourceType).mppw(request);
    }

    @Override
    public Future<Integer> calcQueryCost(SourceType sourceType, RequestMetrics metrics, QueryCostRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.COST,
                metrics,
                plugin -> plugin.calcQueryCost(request));
    }

    @Override
    public Future<StatusQueryResult> status(SourceType sourceType, String topic) {
        return getPlugin(sourceType).status(topic);
    }

    @Override
    public Future<Void> rollback(SourceType sourceType, RequestMetrics metrics, RollbackRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.ROLLBACK,
                metrics,
                plugin -> plugin.rollback(request));
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
    public Future<Void> checkTable(SourceType sourceType,
                                   RequestMetrics metrics,
                                   CheckTableRequest checkTableRequest) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.CHECK,
                metrics,
                plugin -> plugin.checkTable(checkTableRequest));
    }

    @Override
    public Future<Long> checkDataByCount(SourceType sourceType,
                                         RequestMetrics metrics,
                                         CheckDataByCountRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.CHECK,
                metrics,
                plugin -> plugin.checkDataByCount(request));
    }

    @Override
    public Future<Long> checkDataByHashInt32(SourceType sourceType,
                                             RequestMetrics metrics,
                                             CheckDataByHashInt32Request request) {
        return executeWithMetrics(
                sourceType,
                SqlProcessingType.CHECK,
                metrics,
                plugin -> plugin.checkDataByHashInt32(request));
    }

    @Override
    public Future<Void> truncateHistory(SourceType sourceType,
                                        RequestMetrics metrics,
                                        TruncateHistoryRequest params) {
        return executeWithMetrics(
                sourceType,
                SqlProcessingType.TRUNCATE,
                metrics,
                plugin -> plugin.truncateHistory(params));
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
