package io.arenadata.dtm.kafka.core.service.kafka;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.status.PublishStatusEventRequest;

public interface KafkaStatusEventPublisher {
    void publish(PublishStatusEventRequest<?> request, AsyncHandler<Void> handler);
}
