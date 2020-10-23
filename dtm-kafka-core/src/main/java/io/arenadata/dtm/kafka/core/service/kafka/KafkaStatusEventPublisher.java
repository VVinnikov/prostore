package io.arenadata.dtm.kafka.core.service.kafka;

import io.arenadata.dtm.common.status.PublishStatusEventRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface KafkaStatusEventPublisher {
    void publish(PublishStatusEventRequest<?> request, Handler<AsyncResult<Void>> handler);
}
