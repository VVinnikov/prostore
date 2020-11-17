package io.arenadata.dtm.query.execution.core.service.metrics;

import io.vertx.core.eventbus.Message;

public interface MetricsConsumer {

    void consume(Message<String> message);
}
