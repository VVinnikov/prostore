package io.arenadata.dtm.query.execution.core.eddl.service;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlAction;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlQuery;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlRequestContext;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsService;
import io.vertx.core.Future;
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
    public Future<QueryResult> execute(EddlRequestContext context) {
        return paramExtractor.extract(context)
                .compose(eddlQuery -> sendMetricsAndExecute(context, eddlQuery));
    }

    private Future<QueryResult> sendMetricsAndExecute(EddlRequestContext context, EddlQuery eddlQuery) {
        return metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA, SqlProcessingType.EDDL, context.getMetrics())
                .compose(v -> getExecutor(eddlQuery))
                .compose(executor -> executor.execute(eddlQuery));
    }

    private Future<EddlExecutor> getExecutor(EddlQuery eddlQuery) {
        return Future.future(promise -> {
            final EddlExecutor executor = executors.get(eddlQuery.getAction());
            if (executor != null) {
                promise.complete(executor);
            } else {
                promise.fail(new DtmException(
                        String.format("Couldn't find eddl executor for action %s",
                                eddlQuery.getAction())));
            }
        });
    }

}
