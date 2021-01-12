package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.dto.delta.query.RollbackDeltaQuery;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaPostExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service("coreDeltaService")
@Slf4j
public class DeltaServiceImpl implements DeltaService<QueryResult> {

    private final Map<DeltaAction, DeltaExecutor> executors;
    private final DeltaQueryParamExtractor deltaQueryParamExtractor;
    private final MetricsService<RequestMetrics> metricsService;
    private final Map<PostSqlActionType, DeltaPostExecutor> postExecutorMap;

    @Autowired
    public DeltaServiceImpl(DeltaQueryParamExtractor deltaQueryParamExtractor,
                            List<DeltaExecutor> deltaExecutorList,
                            @Qualifier("coreMetricsService") MetricsService<RequestMetrics> metricsService) {

        this.deltaQueryParamExtractor = deltaQueryParamExtractor;
        this.executors = deltaExecutorList.stream()
                .collect(Collectors.toMap(DeltaExecutor::getAction, it -> it));
        this.metricsService = metricsService;
        this.postExecutorMap = new HashMap<>();
    }

    @Override
    public Future<QueryResult> execute(DeltaRequestContext context) {
        if (StringUtils.isEmpty(context.getRequest().getQueryRequest().getDatamartMnemonic())) {
            String errMsg = "Datamart must be not empty!\n" +
                    "For setting datamart you can use the following command: \"USE datamartName\"";
            return Future.failedFuture(new DtmException(errMsg));
        } else {
            return extractDeltaAndExecute(context);
        }
    }

    private Future<QueryResult> extractDeltaAndExecute(DeltaRequestContext context) {
        return Future.future(promise -> {
            deltaQueryParamExtractor.extract(context.getRequest().getQueryRequest())
                    .compose(deltaQuery -> sendMetricsAndExecute(context, deltaQuery))
                    .onComplete(deltaExecHandler -> {
                        if (deltaExecHandler.succeeded()) {
                            QueryResult queryDeltaResult = deltaExecHandler.result();
                            log.debug("Query result: {}, queryResult : {}",
                                    context.getRequest().getQueryRequest(), queryDeltaResult);
                            promise.complete(queryDeltaResult);
                        } else {
                            promise.fail(deltaExecHandler.cause());
                        }
                    });
        });
    }

    private Future<QueryResult> sendMetricsAndExecute(DeltaRequestContext context, DeltaQuery deltaQuery) {
        deltaQuery.setDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic());
        deltaQuery.setRequest(context.getRequest().getQueryRequest());
        if (deltaQuery.getDeltaAction() != DeltaAction.ROLLBACK_DELTA) {
            return executeWithMetrics(context, deltaQuery);
        } else {
            final RollbackDeltaQuery rollbackDeltaQuery = (RollbackDeltaQuery) deltaQuery;
            rollbackDeltaQuery.setRequestMetrics(RequestMetrics.builder()
                    .requestId(context.getMetrics().getRequestId())
                    .startTime(context.getMetrics().getStartTime())
                    .status(RequestStatus.IN_PROCESS)
                    .isActive(true)
                    .build());
            return getExecutor(deltaQuery)
                    .compose(deltaExecutor -> execute(rollbackDeltaQuery, context));
        }
    }

    private Future<QueryResult> executeWithMetrics(DeltaRequestContext context, DeltaQuery deltaQuery) {
        return Future.future((Promise<QueryResult> promise) ->
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA, SqlProcessingType.DELTA, context.getMetrics())
                        .compose(result -> execute(deltaQuery, context))
                        .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                SqlProcessingType.DELTA,
                                context.getMetrics(), promise)));
    }

    private Future<QueryResult> execute(DeltaQuery deltaQuery, DeltaRequestContext context) {
        return Future.future(promise -> getExecutor(deltaQuery)
                .compose(deltaExecutor -> {
                    context.getPostActions().addAll(deltaExecutor.getPostActions());
                    return deltaExecutor.execute(deltaQuery);
                })
                .onSuccess(queryResult -> {
                    executePostActions(context);
                    promise.complete(queryResult);
                })
                .onFailure(promise::fail));
    }

    private Future<DeltaExecutor> getExecutor(DeltaQuery deltaQuery) {
        return Future.future(promise -> {
            final DeltaExecutor executor = executors.get(deltaQuery.getDeltaAction());
            if (executor != null) {
                promise.complete(executor);
            } else {
                promise.fail(new DtmException(String.format("Couldn't find delta executor for action %s",
                        deltaQuery.getDeltaAction())));
            }
        });
    }

    private void executePostActions(DeltaRequestContext context) {
        CompositeFuture.join(context.getPostActions().stream()
                .distinct()
                .map(postType -> Optional.ofNullable(postExecutorMap.get(postType))
                        .map(postExecutor -> postExecutor.execute(context))
                        .orElse(Future.failedFuture(new DtmException(String.format("Not supported delta post executor type [%s]",
                                postType)))))
                .collect(Collectors.toList()))
                .onFailure(error -> log.error(error.getMessage()));
    }

    @Override
    public void addPostExecutor(DeltaPostExecutor executor) {
        postExecutorMap.put(executor.getPostActionType(), executor);
    }

}
