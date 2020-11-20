package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
    public void execute(DeltaRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        if (StringUtils.isEmpty(context.getRequest().getQueryRequest().getDatamartMnemonic())) {
            String errMsg = "Datamart must be not empty!\n" +
                    "For setting datamart you can use the following command: \"USE datamartName\"";
            log.error(errMsg);
            handler.handle(Future.failedFuture(errMsg));
        } else {
            extractDeltaQuery(context)
                    .compose(deltaQuery -> sendMetricsAndExecute(context, deltaQuery))
                    .onComplete(deltaExecHandler -> {
                        if (deltaExecHandler.succeeded()) {
                            QueryResult queryDeltaResult = deltaExecHandler.result();
                            log.debug("Query result: {}, queryResult : {}",
                                    context.getRequest().getQueryRequest(), queryDeltaResult);
                            handler.handle(Future.succeededFuture(queryDeltaResult));
                        } else {
                            log.error(deltaExecHandler.cause().getMessage());
                            handler.handle(Future.failedFuture(deltaExecHandler.cause()));
                        }
                    });
        }
    }

    private Future<DeltaQuery> extractDeltaQuery(DeltaRequestContext context) {
        return Future.future(promise -> deltaQueryParamExtractor.extract(context.getRequest().getQueryRequest(), promise));
    }

    private Future<QueryResult> sendMetricsAndExecute(DeltaRequestContext context, DeltaQuery deltaQuery) {
        return metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                getSqlType(deltaQuery.getDeltaAction()),
                context.getMetrics())
                .compose(v -> execute(context, deltaQuery));
    }

    private Future<QueryResult> execute(DeltaRequestContext context, DeltaQuery deltaQuery) {
        return Future.future((Promise<QueryResult> promise) -> {
            deltaQuery.setDatamart(context.getRequest().getQueryRequest().getDatamartMnemonic());
            deltaQuery.setRequest(context.getRequest().getQueryRequest());
            executors.get(deltaQuery.getDeltaAction())
                    .execute(deltaQuery,
                            metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                    getSqlType(deltaQuery.getDeltaAction()),
                                    context.getMetrics(),
                                    promise));
        });
    }

    private SqlProcessingType getSqlType(DeltaAction deltaAction) {
        if (deltaAction != DeltaAction.ROLLBACK_DELTA) {
            return SqlProcessingType.DELTA;
        }
        return SqlProcessingType.ROLLBACK;
    }
}
