package io.arenadata.dtm.query.execution.core.status.verticle;

import io.arenadata.dtm.common.eventbus.DataTopic;
import io.vertx.core.AbstractVerticle;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(
        value = "core.kafka.status.event.publish.enabled",
        havingValue = "false"
)
@Component
public class MockStatusEventVerticle extends AbstractVerticle {

    @Override
    public void start() {
        vertx.eventBus().consumer(DataTopic.STATUS_EVENT_PUBLISH.getValue(), message -> {});
    }
}
