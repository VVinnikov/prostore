package io.arenadata.dtm.kafka.core.service.kafka;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.status.PublishStatusEventRequest;
import io.arenadata.dtm.kafka.core.configuration.properties.PublishStatusEventProperties;
import io.vertx.core.Future;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Slf4j
@ConditionalOnProperty(
        value = "core.kafka.status.event.publish.enabled",
        havingValue = "true"
)
@Service
public class KafkaStatusEventPublisherImpl implements KafkaStatusEventPublisher {
    private final KafkaProducer<String, String> producer;
    private final PublishStatusEventProperties properties;

    public KafkaStatusEventPublisherImpl(
        @Qualifier("jsonCoreKafkaProducer") KafkaProducer<String, String> producer,
        @Qualifier("publishStatusEventProperties") PublishStatusEventProperties properties
    ) {
        this.producer = producer;
        this.properties = properties;
    }

    @Override
    public void publish(PublishStatusEventRequest<?> request, AsyncHandler<Void> handler) {
        try {
            log.debug("Key [{}] and message [{}] sent to topic [{}]",
                    request.getEventKey(),
                    request.getEventMessage(),
                    properties.getTopic());
            val key = DatabindCodec.mapper().writeValueAsString(request.getEventKey());
            val message = DatabindCodec.mapper().writeValueAsString(request.getEventMessage());
            val record = KafkaProducerRecord.create(properties.getTopic(), key, message);
            producer.send(record, AsyncUtils.succeed(handler, rm -> handler.handleSuccess()));
        } catch (Exception ex) {
            handler.handleError(new DtmException("Error creating status event record", ex));
        }
    }
}
