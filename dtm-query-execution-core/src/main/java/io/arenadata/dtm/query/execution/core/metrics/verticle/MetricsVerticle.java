package io.arenadata.dtm.query.execution.core.metrics.verticle;

import io.arenadata.dtm.common.metrics.MetricsTopic;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsConsumer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MetricsVerticle extends AbstractVerticle {

    private final MetricsConsumer consumer;

    @Autowired
    public MetricsVerticle(MetricsConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer(MetricsTopic.ALL_EVENTS.getValue(), this::allEventsHandler);
    }

    private void allEventsHandler(Message<String> message) {
        consumer.consume(message);
    }
}
