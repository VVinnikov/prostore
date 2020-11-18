package io.arenadata.dtm.query.execution.core.service.eddl.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlAction;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlExecutor;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlQueryParamExtractor;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.plugin.api.eddl.EddlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.EddlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("coreEddlService")
@Slf4j
public class EddlServiceImpl implements EddlService<QueryResult> {

    private final EddlQueryParamExtractor paramExtractor;
    private final Map<EddlAction, EddlExecutor> executors;
    private final MetricsService<RequestMetrics> metricsService;

    @Autowired
    public EddlServiceImpl(EddlQueryParamExtractor paramExtractor,
                           List<EddlExecutor> eddlExecutors,
                           MetricsService<RequestMetrics> metricsService) {
        this.paramExtractor = paramExtractor;
        this.executors = eddlExecutors.stream()
                .collect(Collectors.toMap(EddlExecutor::getAction, it -> it));
        this.metricsService = metricsService;
    }

    @Override
    public void execute(EddlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        paramExtractor.extract(context.getRequest().getQueryRequest(), extractHandler -> {
            if (extractHandler.succeeded()) {
                EddlQuery eddlQuery = extractHandler.result();
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.EDDL,
                        context.getMetrics())
                        .onSuccess(ar -> {
                            executors.get(eddlQuery.getAction()).execute(eddlQuery,
                                    metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                            SqlProcessingType.EDDL,
                                            context.getMetrics(), execHandler -> {
                                                if (execHandler.succeeded()) {
                                                    asyncResultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                                                } else {
                                                    log.error(execHandler.cause().getMessage());
                                                    asyncResultHandler.handle(Future.failedFuture(execHandler.cause()));
                                                }
                                            }));
                        })
                        .onFailure(fail -> asyncResultHandler.handle(Future.failedFuture(fail)));
            } else {
                log.error(extractHandler.cause().getMessage());
                asyncResultHandler.handle(Future.failedFuture(extractHandler.cause()));
            }
        });
    }
}
