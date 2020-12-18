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
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("coreDeltaService")
@Slf4j
public class DeltaServiceImpl implements DeltaService<QueryResult> {

    private final Map<DeltaAction, DeltaExecutor> executors;
    private final DeltaQueryParamExtractor deltaQueryParamExtractor;
    private final MetricsService<RequestMetrics> metricsService;

    @Autowired
    public DeltaServiceImpl(DeltaQueryParamExtractor deltaQueryParamExtractor,
                            List<DeltaExecutor> deltaExecutorList,
                            @Qualifier("coreMetricsService") MetricsService<RequestMetrics> metricsService) {

        this.deltaQueryParamExtractor = deltaQueryParamExtractor;
        this.executors = deltaExecutorList.stream()
                .collect(Collectors.toMap(DeltaExecutor::getAction, it -> it));
        this.metricsService = metricsService;
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
                    .compose(deltaExecutor -> execute(rollbackDeltaQuery));
        }
    }

    private Future<QueryResult> executeWithMetrics(DeltaRequestContext context, DeltaQuery deltaQuery) {
        return Future.future((Promise<QueryResult> promise) ->
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA, SqlProcessingType.DELTA, context.getMetrics())
                        .compose(result -> execute(deltaQuery))
                        .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                SqlProcessingType.DELTA,
                                context.getMetrics(), promise)));
    }

    private Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return getExecutor(deltaQuery)
                .compose(deltaExecutor -> deltaExecutor.execute(deltaQuery));
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

}
