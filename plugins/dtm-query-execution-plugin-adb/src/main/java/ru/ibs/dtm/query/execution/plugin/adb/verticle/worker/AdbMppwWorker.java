package ru.ibs.dtm.query.execution.plugin.adb.verticle.worker;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.handler.AdbMppwHandler;

import java.util.Map;

@Slf4j
public class AdbMppwWorker extends AbstractVerticle {

    private final Map<String, MppwKafkaRequestContext> requestMap;
    private final AdbMppwHandler mppwTransferDataHandler;

    public AdbMppwWorker(Map<String, MppwKafkaRequestContext> requestMap, AdbMppwHandler mppwTransferDataHandler) {
        this.requestMap = requestMap;
        this.mppwTransferDataHandler = mppwTransferDataHandler;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer(MppwTopic.KAFKA_START.getValue(), this::handleStartMppwKafka);
        vertx.eventBus().consumer(MppwTopic.KAFKA_STOP.getValue(), this::handleStopMppwKafka);
        vertx.eventBus().consumer(MppwTopic.KAFKA_TRANSFER_DATA.getValue(), this::handleStartTransferData);
    }

    private void handleStartMppwKafka(Message<String> requestMessage) {
        final RestLoadRequest restLoadRequest =
                Json.decodeValue(((JsonObject) Json.decodeValue(requestMessage.body()))
                        .getJsonObject("restLoadRequest").toString(), RestLoadRequest.class);
        final MppwTransferDataRequest transferDataRequest =
                Json.decodeValue(((JsonObject) Json.decodeValue(requestMessage.body()))
                        .getJsonObject("mppwTransferDataRequest").toString(), MppwTransferDataRequest.class);
        MppwKafkaRequestContext kafkaRequestContext = new MppwKafkaRequestContext(restLoadRequest, transferDataRequest);
        requestMap.put(kafkaRequestContext.getRestLoadRequest().getRequestId(), kafkaRequestContext);
        vertx.eventBus().publish(MppwTopic.KAFKA_TRANSFER_DATA.getValue(),
                kafkaRequestContext.getRestLoadRequest().getRequestId());
    }

    private void handleStopMppwKafka(Message<String> requestMessage) {
        String requestId = requestMessage.body();
        requestMap.remove(requestId);
        requestMessage.reply(requestId);
    }

    private void handleStartTransferData(Message<String> requestMessage) {
        String requestId = requestMessage.body();
        final MppwKafkaRequestContext requestContext = requestMap.get(requestId);
        if (requestContext != null) {
            mppwTransferDataHandler.handle(requestContext)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            vertx.eventBus().publish(MppwTopic.KAFKA_TRANSFER_DATA.getValue(),
                                    requestContext.getRestLoadRequest().getRequestId());
                            log.debug("Request executed successfully: {}", requestContext.getRestLoadRequest());
                        } else {
                            requestMap.remove(requestId);
                            log.error("Error executing request: {}", requestContext.getRestLoadRequest(), ar.cause());
                        }
                    });
        }
    }
}
