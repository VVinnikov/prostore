package io.arenadata.dtm.query.execution.plugin.adb.verticle.worker;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.handler.AdbMppwHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class AdbMppwWorker extends AbstractVerticle {

    private final Map<String, MppwKafkaRequestContext> requestMap;
    private final Map<String, Future> resultMap;
    private final AdbMppwHandler mppwTransferDataHandler;

    public AdbMppwWorker(Map<String, MppwKafkaRequestContext> requestMap, Map<String, Future> resultMap,
                         AdbMppwHandler mppwTransferDataHandler) {
        this.requestMap = requestMap;
        this.resultMap = resultMap;
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
        log.debug("Received request for starting mppw kafka loading: {}", kafkaRequestContext);
        requestMap.put(kafkaRequestContext.getRestLoadRequest().getRequestId(), kafkaRequestContext);
        vertx.eventBus().send(MppwTopic.KAFKA_TRANSFER_DATA.getValue(),
                kafkaRequestContext.getRestLoadRequest().getRequestId());
    }

    private void handleStopMppwKafka(Message<String> requestMessage) {
        String requestId = requestMessage.body();
        Future transferPromise = resultMap.remove(requestId);
        transferPromise.onComplete(ar -> {
            requestMap.remove(requestId);
            requestMessage.reply(requestId);
        });
    }

    private void handleStartTransferData(Message<String> requestMessage) {
        String requestId = requestMessage.body();
        final MppwKafkaRequestContext requestContext = requestMap.get(requestId);
        if (requestContext != null) {
            log.debug("Received requestId: {}, found requestContext in map: {}", requestId, requestContext);
            Promise promise = Promise.promise();
            resultMap.put(requestId, promise.future());
            mppwTransferDataHandler.handle(requestContext)
                    .onSuccess(s -> {
                        log.debug("Request executed successfully: {}", requestContext.getRestLoadRequest());
                        vertx.eventBus().send(MppwTopic.KAFKA_TRANSFER_DATA.getValue(),
                                requestContext.getRestLoadRequest().getRequestId());
                        promise.complete();
                    })
                    .onFailure(fail -> {
                        log.error("Error executing request: {}", requestContext.getRestLoadRequest(), fail);
                        promise.fail(fail);
                    });
        }
    }
}
