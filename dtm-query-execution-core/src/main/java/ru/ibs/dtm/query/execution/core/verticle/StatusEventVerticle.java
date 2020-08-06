package ru.ibs.dtm.query.execution.core.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.eventbus.DataHeader;
import ru.ibs.dtm.common.eventbus.DataTopic;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaStatusEventPublisher;
import ru.ibs.dtm.query.execution.core.registry.StatusEventFactoryRegistry;

@Slf4j
@Component
public class StatusEventVerticle extends AbstractVerticle {

    private final KafkaStatusEventPublisher kafkaStatusEventPublisher;
    private final StatusEventFactoryRegistry statusEventFactoryRegistry;

    public StatusEventVerticle(
        KafkaStatusEventPublisher kafkaStatusEventPublisher,
        StatusEventFactoryRegistry statusEventFactoryRegistry
    ) {
        this.kafkaStatusEventPublisher = kafkaStatusEventPublisher;
        this.statusEventFactoryRegistry = statusEventFactoryRegistry;
    }

    @Override
    public void start() {
        vertx.eventBus()
            .consumer(DataTopic.STATUS_EVENT_PUBLISH.getValue(), this::onPublishStatusEvent);
    }

    private void onPublishStatusEvent(Message<String> statusMessage) {
        try {
            val eventCode = StatusEventCode.valueOf(statusMessage.headers().get(DataHeader.STATUS_EVENT_CODE.getValue()));
            val datamart = statusMessage.headers().get(DataHeader.DATAMART.getValue());
            val eventRequest = statusEventFactoryRegistry.get(eventCode).create(datamart, statusMessage.body());
            kafkaStatusEventPublisher.publish(eventRequest, ar -> {
                if (ar.succeeded()) {
                    log.debug("StatusEvent publish completed [{}]", eventRequest);
                } else {
                    log.error("StatusEvent publish error", ar.cause());
                }
            });
        } catch (Exception e) {
            log.error("StatusEvent publish error", e);
        }
    }
}
