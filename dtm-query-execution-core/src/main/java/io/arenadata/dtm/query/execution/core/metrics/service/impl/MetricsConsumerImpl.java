package io.arenadata.dtm.query.execution.core.metrics.service.impl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.core.metrics.factory.MetricsEventFactory;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsConsumer;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsProcessingService;
import io.vertx.core.eventbus.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MetricsConsumerImpl implements MetricsConsumer {

    private final MetricsEventFactory<RequestMetrics> eventFactory;
    private final MetricsProcessingService<RequestMetrics> operationMetricsService;

    @Autowired
    public MetricsConsumerImpl(MetricsEventFactory<RequestMetrics> eventFactory,
                               MetricsProcessingService<RequestMetrics> operationMetricsService) {
        this.operationMetricsService = operationMetricsService;
        this.eventFactory = eventFactory;
    }

    @Override
    public void consume(Message<String> message) {
        final RequestMetrics requestMetrics = eventFactory.create(message.body());
        operationMetricsService.process(requestMetrics);
    }
}
