package io.arenadata.dtm.query.execution.plugin.adb.verticle.worker;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
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

    public static final int ERROR_CODE = 400;
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
        final MppwKafkaLoadRequest loadRequest =
                Json.decodeValue(((JsonObject) Json.decodeValue(requestMessage.body()))
                        .getJsonObject("mppwKafkaLoadRequest").toString(), MppwKafkaLoadRequest.class);
        final MppwTransferDataRequest transferDataRequest =
                Json.decodeValue(((JsonObject) Json.decodeValue(requestMessage.body()))
                        .getJsonObject("mppwTransferDataRequest").toString(), MppwTransferDataRequest.class);
        MppwKafkaRequestContext kafkaRequestContext = new MppwKafkaRequestContext(loadRequest, transferDataRequest);
        log.debug("Received request for starting mppw kafka loading: {}", kafkaRequestContext);
        requestMap.put(kafkaRequestContext.getMppwKafkaLoadRequest().getRequestId(), kafkaRequestContext);
        vertx.eventBus().send(MppwTopic.KAFKA_TRANSFER_DATA.getValue(),
                kafkaRequestContext.getMppwKafkaLoadRequest().getRequestId());
    }

    private void handleStopMppwKafka(Message<String> requestMessage) {
        String requestId = requestMessage.body();
        Future<?> transferPromise = resultMap.remove(requestId);
        if (transferPromise != null) {
            transferPromise.onComplete(ar -> {
                requestMap.remove(requestId);
                if (ar.failed()) {
                    requestMessage.fail(ERROR_CODE, ar.cause().getMessage());
                } else {
                    requestMessage.reply(requestId);
                }
            });
        } else {
            requestMessage.reply(requestId);
        }
    }

    private void handleStartTransferData(Message<String> requestMessage) {
        String requestId = requestMessage.body();
        final MppwKafkaRequestContext requestContext = requestMap.get(requestId);
        if (requestContext != null) {
            log.debug("Received requestId: {}, found requestContext in map: {}", requestId, requestContext);
            Promise<?> promise = Promise.promise();
            resultMap.put(requestId, promise.future());
            mppwTransferDataHandler.handle(requestContext)
                    .onSuccess(s -> {
                        log.debug("Request executed successfully: {}", requestContext.getMppwKafkaLoadRequest());
                        vertx.eventBus().send(MppwTopic.KAFKA_TRANSFER_DATA.getValue(),
                                requestContext.getMppwKafkaLoadRequest().getRequestId());
                        promise.complete();
                    })
                    .onFailure(err -> {
                        log.error("Error transferring data: {}", requestContext.getMppwKafkaLoadRequest(), err);
                        promise.fail(err);
                    });
        }
    }
}
