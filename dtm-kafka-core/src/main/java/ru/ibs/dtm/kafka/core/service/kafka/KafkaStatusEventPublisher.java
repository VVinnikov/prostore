package ru.ibs.dtm.kafka.core.service.kafka;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.status.PublishStatusEventRequest;

public interface KafkaStatusEventPublisher {
    void publish(PublishStatusEventRequest<?> request, Handler<AsyncResult<Void>> handler);
}