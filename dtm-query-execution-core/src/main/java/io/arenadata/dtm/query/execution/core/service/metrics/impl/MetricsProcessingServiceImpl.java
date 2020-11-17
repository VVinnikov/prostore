package io.arenadata.dtm.query.execution.core.service.metrics.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsProcessingService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Objects;

import static io.arenadata.dtm.query.execution.core.utils.MetricsUtil.*;

@Service
@Slf4j
public class MetricsProcessingServiceImpl implements MetricsProcessingService<RequestMetrics> {

    private final MeterRegistry meterRegistry;

    @Autowired
    public MetricsProcessingServiceImpl(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        initCounters();
    }

    @Override
    public void process(RequestMetrics metricsValue) {
        log.debug("Received metrics request: {}", metricsValue);
        Objects.requireNonNull(meterRegistry
                .find(REQUESTS_AMOUNT_NAME)
                .tags(ACTION_TYPE, metricsValue.getActionType().name(),
                        SOURCE_TYPE, metricsValue.getSourceType().name())
                .counter())
                .increment();
    }

    private void initCounters() {
        Arrays.stream(SqlProcessingType.values()).forEach(actionType -> {
            Arrays.stream(SourceType.values()).forEach(st ->
                    this.meterRegistry.counter(
                            REQUESTS_AMOUNT_NAME,
                            ACTION_TYPE,
                            actionType.name(),
                            SOURCE_TYPE,
                            st.name())
            );
        });
    }
}
