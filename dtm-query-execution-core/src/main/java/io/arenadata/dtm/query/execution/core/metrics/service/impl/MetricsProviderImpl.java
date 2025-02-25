/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.metrics.service.impl;

import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.metrics.dto.*;
import io.arenadata.dtm.query.execution.core.metrics.repository.ActiveRequestsRepository;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsProvider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.metrics.utils.MetricsUtil.*;

@Component
public class MetricsProviderImpl implements MetricsProvider {

    private final MeterRegistry meterRegistry;
    private final ActiveRequestsRepository<RequestMetrics> activeRequestsRepository;
    private final MetricsSettings metricsSettings;

    @Autowired
    public MetricsProviderImpl(MeterRegistry meterRegistry,
                               @Qualifier("mapActiveRequestsRepository")
                                       ActiveRequestsRepository<RequestMetrics> activeRequestsRepository,
                               MetricsSettings metricsSettings) {
        this.meterRegistry = meterRegistry;
        this.activeRequestsRepository = activeRequestsRepository;
        this.metricsSettings = metricsSettings;
        initRequestsCounters(REQUESTS_AMOUNT);
        initRequestsTimers(REQUESTS_TIME);
    }

    @Override
    public ResultMetrics get() {
        return new ResultMetrics(metricsSettings.isEnabled(), getRequestsAmountStats());
    }

    private List<RequestStats> getRequestsAmountStats() {
        final Map<SqlProcessingType, List<RequestMetrics>> activeRequestMap =
                activeRequestsRepository.getList().stream()
                        .collect(Collectors.groupingBy(RequestMetrics::getActionType));
        return Arrays.stream(SqlProcessingType.values())
                .map(st -> new RequestStats(st,
                        createRequestAmountMetrics(st),
                        createRequestsActiveMetrics(getRequestMetricsList(activeRequestMap, st))
                )).collect(Collectors.toList());
    }

    private RequestsAllMetrics createRequestAmountMetrics(SqlProcessingType st) {
        return new RequestsAllMetrics(meterRegistry
                .find(REQUESTS_AMOUNT)
                .tag(ACTION_TYPE, st.name())
                .counters().stream()
                .mapToLong(c -> (long) c.count())
                .reduce(0, Long::sum),
                Arrays.stream(SourceType.values()).map(s -> {
                    final Timer timer = meterRegistry
                            .find(REQUESTS_TIME)
                            .tags(ACTION_TYPE, st.name(), SOURCE_TYPE, s.name())
                            .timer();
                    final Counter counter = Objects.requireNonNull(meterRegistry
                            .find(REQUESTS_AMOUNT)
                            .tags(ACTION_TYPE, st.name(), SOURCE_TYPE, s.name())
                            .counter());
                    return new AllStats(s, new CountMetrics((long) counter.count()),
                            new TimeMetrics(timer.count(),
                                    (long) timer.totalTime(TimeUnit.MILLISECONDS),
                                    (long) timer.mean(TimeUnit.MILLISECONDS),
                                    (long) timer.max(TimeUnit.MILLISECONDS))
                    );
                }).collect(Collectors.toList()));
    }

    private RequestsActiveMetrics createRequestsActiveMetrics(List<RequestMetrics> requestMetrics) {
        return requestMetrics.stream().map(rl ->
                new RequestsActiveMetrics(
                        (long) requestMetrics.size(),
                        getActiveStats(requestMetrics)
                )).findFirst().orElse(null);
    }

    private List<ActiveStats> getActiveStats(List<RequestMetrics> requestMetrics) {
        final Map<SourceType, List<RequestMetrics>> typeListMap =
                requestMetrics.stream().collect(Collectors.groupingBy(RequestMetrics::getSourceType));
        return typeListMap.entrySet().stream()
                .map(k -> new ActiveStats(k.getKey(),
                        TimeMetrics.builder()
                                .count((long) k.getValue().size())
                                .totalTimeMs(calcActiveTotalTime(k.getValue()))
                                .build()
                )).collect(Collectors.toList());
    }

    private long calcActiveTotalTime(List<RequestMetrics> requestMetrics) {
        return requestMetrics.stream().map(r ->
                Duration.between(r.getStartTime(),
                                LocalDateTime.now(CoreConstants.CORE_ZONE_ID))
                        .toMillis()).reduce(0L, Long::sum);
    }

    private List<RequestMetrics> getRequestMetricsList(Map<SqlProcessingType, List<RequestMetrics>> activeRequestMap,
                                                       SqlProcessingType st) {
        final List<RequestMetrics> requestMetrics = activeRequestMap.get(st);
        return requestMetrics == null ? Collections.emptyList() : requestMetrics;
    }

    private void initRequestsCounters(String counterName) {
        Arrays.stream(SqlProcessingType.values()).forEach(actionType -> {
            Arrays.stream(SourceType.values()).forEach(st ->
                    meterRegistry.counter(
                            counterName,
                            ACTION_TYPE,
                            actionType.name(),
                            SOURCE_TYPE,
                            st.name())
            );
        });
    }

    private void initRequestsTimers(String timerName) {
        Arrays.stream(SqlProcessingType.values()).forEach(actionType -> {
            Arrays.stream(SourceType.values()).forEach(st ->
                    meterRegistry.timer(timerName,
                            ACTION_TYPE,
                            actionType.name(),
                            SOURCE_TYPE,
                            st.name())
            );
        });
    }

    @Override
    public void clear() {
        activeRequestsRepository.deleteAll();
        meterRegistry.clear();
        initRequestsCounters(REQUESTS_AMOUNT);
        initRequestsTimers(REQUESTS_TIME);
    }

}
