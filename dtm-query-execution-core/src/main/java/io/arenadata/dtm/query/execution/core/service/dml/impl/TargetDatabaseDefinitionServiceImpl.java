package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.exception.query.NoSingleDataSourceContainsAllEntitiesException;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.TargetDatabaseDefinitionService;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class TargetDatabaseDefinitionServiceImpl implements TargetDatabaseDefinitionService {

    private final DataSourcePluginService pluginService;
    private final EntityDao entityDao;
    private final DtmConfig dtmSettings;
    private final AppConfiguration configuration;//FIXME get env from dtmConfig

    @Autowired
    public TargetDatabaseDefinitionServiceImpl(DataSourcePluginService pluginService,
                                               EntityDao entityDao,
                                               DtmConfig dtmSettings, AppConfiguration configuration) {
        this.pluginService = pluginService;
        this.entityDao = entityDao;
        this.dtmSettings = dtmSettings;
        this.configuration = configuration;
    }

    @Override
    public Future<Set<SourceType>> getAcceptableSourceTypes(QuerySourceRequest request) {
        return getEntities(request)
                .compose(entities -> Future.future(promise -> promise.complete(getSourceTypes(entities))));
    }

    @Override
    public Future<SourceType> getSourceTypeWithLeastQueryCost(Set<SourceType> sourceTypes, QuerySourceRequest request) {
        return Future.future(promise -> CompositeFuture.join(sourceTypes.stream()
                .map(sourceType -> calcQueryCostInPlugin(request, sourceType))
                .collect(Collectors.toList()))
                .onSuccess(ar -> {
                    SourceType sourceType = ar.list().stream()
                            //this sort is needed to get the first source type - ADB if all costs will be equal
                            .sorted(Comparator.comparing(st -> ((Pair<SourceType, Integer>) st).getKey().ordinal()))
                            .map(res -> (Pair<SourceType, Integer>) res)
                            .min(Comparator.comparingInt(Pair::getValue))
                            .map(Pair::getKey)
                            .orElse(null);
                    promise.complete(sourceType);
                })
                .onFailure(promise::fail)
        );
    }

    private Future<List<Entity>> getEntities(QuerySourceRequest request) {
        return Future.future(promise -> {
            List<Future> entityFutures = new ArrayList<>();
            request.getLogicalSchema().forEach(datamart ->
                    datamart.getEntities().forEach(entity ->
                            entityFutures.add(entityDao.getEntity(datamart.getMnemonic(), entity.getName()))
                    ));

            CompositeFuture.join(entityFutures)
                    .onSuccess(entities -> promise.complete(entities.list()))
                    .onFailure(promise::fail);
        });
    }

    private Set<SourceType> getSourceTypes(List<Entity> entities) {
        final Set<SourceType> stResult = getCommonSourceTypes(entities);
        if (stResult.isEmpty()) {
            throw new NoSingleDataSourceContainsAllEntitiesException();
        } else {
            return stResult;
        }
    }

    private Future<Object> calcQueryCostInPlugin(QuerySourceRequest request, SourceType sourceType) {
        return Future.future(p -> {
            val costRequest = new QueryCostRequest(request.getQueryRequest().getRequestId(),
                    configuration.getEnvName(),
                    request.getQueryRequest().getDatamartMnemonic(),
                    request.getLogicalSchema());
            pluginService.calcQueryCost(sourceType, createRequestMetrics(request), costRequest)
                    .onComplete(costHandler -> {
                        if (costHandler.succeeded()) {
                            p.complete(Pair.of(sourceType, costHandler.result()));
                        } else {
                            p.fail(costHandler.cause());
                        }
                    });
        });
    }

    private Set<SourceType> getCommonSourceTypes(List<Entity> entities) {
        if (entities.isEmpty()) {
            return new HashSet<>();
        } else {
            Set<SourceType> stResult = entities.get(0).getDestination().stream()
                    .collect(Collectors.toCollection(HashSet::new));
            entities.forEach(e -> stResult.retainAll(e.getDestination()));
            return stResult;
        }
    }

    private RequestMetrics createRequestMetrics(QuerySourceRequest request) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(dtmSettings.getTimeZone()))
                .requestId(request.getQueryRequest().getRequestId())
                .sourceType(SourceType.INFORMATION_SCHEMA)
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }
}
