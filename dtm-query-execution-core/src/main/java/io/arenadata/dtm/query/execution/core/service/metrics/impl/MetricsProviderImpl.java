package io.arenadata.dtm.query.execution.core.service.metrics.impl;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dto.metrics.BaseAmountMetrics;
import io.arenadata.dtm.query.execution.core.dto.metrics.ResultMetrics;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsProvider;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.utils.MetricsUtil.*;

@Component
public class MetricsProviderImpl implements MetricsProvider {

    private final MeterRegistry registry;

    @Autowired
    public MetricsProviderImpl(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public ResultMetrics get() {
        Map<SqlProcessingType, BaseAmountMetrics> amountMap = new HashMap<>();
        Arrays.stream(SqlProcessingType.values()).forEach(st -> {
            amountMap.put(st, new BaseAmountMetrics(registry
                    .find(REQUESTS_AMOUNT_NAME)
                    .tag(ACTION_TYPE, st.name())
                    .counters().stream()
                    .mapToLong(c -> new Double(c.count()).longValue())
                    .reduce(0, Long::sum),
                    Arrays.stream(SourceType.values()).collect(Collectors.toMap(s -> s, s ->
                            new Double(Objects.requireNonNull(registry
                                    .find(REQUESTS_AMOUNT_NAME)
                                    .tags(ACTION_TYPE, st.name(), SOURCE_TYPE, s.name())
                                    .counter())
                                    .count()).longValue()
                    ))));
        });
        return new ResultMetrics(amountMap);
    }
}
