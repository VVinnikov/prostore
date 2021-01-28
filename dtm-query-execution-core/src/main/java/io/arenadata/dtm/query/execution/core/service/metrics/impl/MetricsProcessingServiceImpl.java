package io.arenadata.dtm.query.execution.core.service.metrics.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dao.metrics.ActiveRequestsRepository;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsProcessingService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

import static io.arenadata.dtm.query.execution.core.utils.MetricsUtil.*;

@Service
@Slf4j
public class MetricsProcessingServiceImpl implements MetricsProcessingService<RequestMetrics> {

    private final MeterRegistry meterRegistry;
    private final ActiveRequestsRepository<RequestMetrics> activeRequestsRepository;

    @Autowired
    public MetricsProcessingServiceImpl(MeterRegistry meterRegistry,
                                        @Qualifier("mapActiveRequestsRepository")
                                                ActiveRequestsRepository<RequestMetrics> activeRequestsRepository) {
        this.meterRegistry = meterRegistry;
        this.activeRequestsRepository = activeRequestsRepository;
    }

    @Override
    public void process(RequestMetrics metricsValue) {
        log.debug("Received metrics request: {}", metricsValue);
        if (metricsValue.isActive()) {
            this.activeRequestsRepository.add(metricsValue);
        } else {
            this.activeRequestsRepository.remove(metricsValue);
            getRequestsCounter(metricsValue, REQUESTS_AMOUNT).increment();
            getRequestsTimer(metricsValue, REQUESTS_TIME)
                    .record(Duration.between(metricsValue.getStartTime(),
                            metricsValue.getEndTime()));
        }
    }

    private Timer getRequestsTimer(RequestMetrics metricsValue, String requestsTimeName) {
        return Objects.requireNonNull(meterRegistry
                .find(requestsTimeName)
                .tags(ACTION_TYPE, metricsValue.getActionType().name(),
                        SOURCE_TYPE, metricsValue.getSourceType().name())
                .timer());
    }

    private Counter getRequestsCounter(RequestMetrics metricsValue, String requestsCounterName) {
        return Objects.requireNonNull(meterRegistry
                .find(requestsCounterName)
                .tags(ACTION_TYPE, metricsValue.getActionType().name(),
                        SOURCE_TYPE, metricsValue.getSourceType().name())
                .counter());
    }
}
