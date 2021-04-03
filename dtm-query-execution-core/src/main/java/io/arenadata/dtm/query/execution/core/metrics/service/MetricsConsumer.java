package io.arenadata.dtm.query.execution.core.metrics.service;

import io.vertx.core.eventbus.Message;

public interface MetricsConsumer {

    void consume(Message<String> message);
}
