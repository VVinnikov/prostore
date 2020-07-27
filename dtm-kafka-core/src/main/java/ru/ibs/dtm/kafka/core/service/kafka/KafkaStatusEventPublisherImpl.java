package ru.ibs.dtm.kafka.core.service.kafka;import io.vertx.core.AsyncResult;import io.vertx.core.Future;import io.vertx.core.Handler;import io.vertx.core.json.jackson.DatabindCodec;import io.vertx.kafka.client.producer.KafkaProducer;import io.vertx.kafka.client.producer.KafkaProducerRecord;import lombok.extern.slf4j.Slf4j;import lombok.val;import org.springframework.beans.factory.annotation.Qualifier;import org.springframework.stereotype.Service;import ru.ibs.dtm.async.AsyncUtils;import ru.ibs.dtm.common.status.PublishStatusEventRequest;import ru.ibs.dtm.kafka.core.configuration.properties.PublishStatusEventProperties;@Slf4j@Servicepublic class KafkaStatusEventPublisherImpl implements KafkaStatusEventPublisher {    private final KafkaProducer<String, String> producer;    private final PublishStatusEventProperties properties;    public KafkaStatusEventPublisherImpl(        @Qualifier("jsonCoreKafkaProducer") KafkaProducer<String, String> producer,        PublishStatusEventProperties properties    ) {        this.producer = producer;        this.properties = properties;    }    @Override    public void publish(PublishStatusEventRequest<?> request, Handler<AsyncResult<Void>> handler) {        try {            log.debug("send key [{}] and message [{}] to topic [{}]", request.getEventKey(), request.getEventMessage(), properties.getTopic());            val key = DatabindCodec.mapper().writeValueAsString(request.getEventKey());            val message = DatabindCodec.mapper().writeValueAsString(request.getEventMessage());            val record = KafkaProducerRecord.create(properties.getTopic(), key, message);            producer.send(record, AsyncUtils.succeed(handler, rm -> handler.handle(Future.succeededFuture())));        } catch (Exception ex) {            log.error("Error serialize message", ex);            handler.handle(Future.failedFuture(ex));        }    }}