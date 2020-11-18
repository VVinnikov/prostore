package io.arenadata.dtm.query.execution.core.service.metrics.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dao.metrics.ActiveRequestsRepository;
import io.arenadata.dtm.query.execution.core.dto.metrics.*;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsProvider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.utils.MetricsUtil.*;

@Component
@Slf4j
public class MetricsProviderImpl implements MetricsProvider {

    private final MeterRegistry registry;
    private final ActiveRequestsRepository<RequestMetrics> activeRequestsRepository;
    private final DtmConfig dtmSettings;

    @Autowired
    public MetricsProviderImpl(MeterRegistry registry,
                               @Qualifier("mapActiveRequestsRepository")
                                       ActiveRequestsRepository<RequestMetrics> activeRequestsRepository,
                               DtmConfig dtmSettings) {
        this.registry = registry;
        this.activeRequestsRepository = activeRequestsRepository;
        this.dtmSettings = dtmSettings;
    }

    @Override
    public ResultMetrics get() {
        return new ResultMetrics(getRequestsAmountStats());
    }

    private List<RequestStats> getRequestsAmountStats() {
        final Map<SqlProcessingType, List<RequestMetrics>> activeRequestMap =
                activeRequestsRepository.getList().stream()
                        .collect(Collectors.groupingBy(RequestMetrics::getActionType));
        return Arrays.stream(SqlProcessingType.values()).map(st ->
                new RequestStats(st,
                        createRequestAmountMetrics(st),
                        createRequestsActiveMetrics(getRequestMetricsList(activeRequestMap, st))
                )).collect(Collectors.toList());
    }

    private RequestsAllMetrics createRequestAmountMetrics(SqlProcessingType st) {
        return new RequestsAllMetrics(registry
                .find(REQUESTS_AMOUNT)
                .tag(ACTION_TYPE, st.name())
                .counters().stream()
                .mapToLong(c -> new Double(c.count()).longValue())
                .reduce(0, Long::sum),
                Arrays.stream(SourceType.values()).map(s -> {
                    final Timer timer = registry
                            .find(REQUESTS_TIME)
                            .tags(ACTION_TYPE, st.name(), SOURCE_TYPE, s.name())
                            .timer();
                    final Counter counter = Objects.requireNonNull(registry
                            .find(REQUESTS_AMOUNT)
                            .tags(ACTION_TYPE, st.name(), SOURCE_TYPE, s.name())
                            .counter());
                    return new AllStats(s, new CountMetrics(new Double(counter
                            .count()).longValue()),
                            new TimeMetrics(new Double(timer.count()).longValue(),
                                    new Double(timer.totalTime(TimeUnit.MILLISECONDS)).longValue(),
                                    new Double(timer.mean(TimeUnit.MILLISECONDS)).longValue(),
                                    new Double(timer.max(TimeUnit.MILLISECONDS)).longValue())
                    );
                }).collect(Collectors.toList()));
    }

    private RequestsActiveMetrics createRequestsActiveMetrics(List<RequestMetrics> requestMetrics) {
        return requestMetrics.stream().map(rl ->
                new RequestsActiveMetrics(
                        Integer.valueOf(requestMetrics.size()).longValue(),
                        getActiveStats(requestMetrics)
                )).findFirst().orElse(null);
    }

    private List<ActiveStats> getActiveStats(List<RequestMetrics> requestMetrics) {
        final Map<SourceType, List<RequestMetrics>> typeListMap =
                requestMetrics.stream().collect(Collectors.groupingBy(RequestMetrics::getSourceType));
        return typeListMap.entrySet().stream().map(k ->
                new ActiveStats(k.getKey(),
                        TimeMetrics.builder()
                                .count(Integer.valueOf(k.getValue().size()).longValue())
                                .totalTimeMs(calcActiveTotalTime(k.getValue()))
                                .build()
                )).collect(Collectors.toList());
    }

    private long calcActiveTotalTime(List<RequestMetrics> requestMetrics) {
        return requestMetrics.stream().map(r ->
                Duration.between(r.getStartTime(),
                        LocalDateTime.now(dtmSettings.getTimeZone()))
                        .toMillis()).reduce(0L, Long::sum);
    }

    private List<RequestMetrics> getRequestMetricsList(Map<SqlProcessingType, List<RequestMetrics>> activeRequestMap, SqlProcessingType st) {
        final List<RequestMetrics> requestMetrics = activeRequestMap.get(st);
        return requestMetrics == null ? Collections.emptyList() : requestMetrics;
    }

}
