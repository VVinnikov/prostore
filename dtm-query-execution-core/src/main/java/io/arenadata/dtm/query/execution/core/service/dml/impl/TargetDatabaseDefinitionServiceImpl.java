package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.TargetDatabaseDefinitionService;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.newHashSet;

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
    public Future<QuerySourceRequest> getTargetSource(QuerySourceRequest request, SqlNode query) {
        return getEntitiesSourceTypes(request)
                .compose(entities -> defineTargetSourceType(entities, request))
                .map(sourceType -> {
                    val queryRequestWithSourceType = request.getQueryRequest().copy();
                    request.setSourceType(sourceType);
                    return QuerySourceRequest.builder()
                            .queryRequest(queryRequestWithSourceType)
                            .logicalSchema(request.getLogicalSchema())
                            .queryTemplate(request.getQueryTemplate())
                            .metadata(request.getMetadata())
                            .sourceType(sourceType)
                            .query(query)
                            .build();
                });
    }

    private Future<List<Entity>> getEntitiesSourceTypes(QuerySourceRequest request) {
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

    private Future<SourceType> defineTargetSourceType(List<Entity> entities, QuerySourceRequest request) {
        return Future.future((Promise<SourceType> promise) -> {
            Set<SourceType> sourceTypes = getSourceTypes(request, entities);
            if (sourceTypes.size() == 1) {
                promise.complete(sourceTypes.iterator().next());
            } else {
                getTargetSourceByCalcQueryCost(sourceTypes, request)
                        .onComplete(promise);
            }
        });
    }

    private Set<SourceType> getSourceTypes(QuerySourceRequest request, List<Entity> entities) {
        final Set<SourceType> stResult = getCommonSourceTypes(entities);
        if (stResult.isEmpty()) {
            throw new DtmException("Tables have no datasource in common");
        } else if (request.getSourceType() != null) {
            if (!stResult.contains(request.getSourceType())) {
                throw new DtmException(String.format("Tables common datasources does not include %s",
                        request.getSourceType()));
            } else {
                return newHashSet(request.getSourceType());
            }
        } else {
            return stResult;
        }
    }

    private Set<SourceType> getCommonSourceTypes(List<Entity> entities) {
        if (entities.isEmpty()) {
            return new HashSet<>();
        } else {
            Set<SourceType> stResult = entities.get(0).getDestination();
            entities.forEach(e -> stResult.retainAll(e.getDestination()));
            return stResult;
        }
    }

    private Future<SourceType> getTargetSourceByCalcQueryCost(Set<SourceType> sourceTypes, QuerySourceRequest request) {
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
