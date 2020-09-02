package ru.ibs.dtm.query.execution.plugin.adb.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.eventbus.DataTopic;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.handler.AdbMppwStartHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AdbMppwVerticle extends AbstractVerticle {

    private final Map<String, MppwKafkaRequestContext> requestMap = new ConcurrentHashMap<>();
    private final AdbMppwStartHandler mppwStartHandler;

    @Autowired
    public AdbMppwVerticle(AdbMppwStartHandler mppwStartHandler) {
        this.mppwStartHandler = mppwStartHandler;
    }

    @Override
    public void start() {
        vertx.eventBus().consumer(DataTopic.MPPW_START.getValue(), this::handle);
        vertx.eventBus().consumer(DataTopic.MPPW_KAFKA_START.getValue(), this::handleMppwKafka);
    }

    private void handle(Message<String> requestMessage) {
        MppwKafkaRequestContext kafkaRequestContext = (MppwKafkaRequestContext) Json.decodeValue(requestMessage.body());
        if (kafkaRequestContext.getRestLoadRequest().isStart()) {
            requestMap.put(kafkaRequestContext.getRestLoadRequest().getRequestId(), kafkaRequestContext);
            vertx.eventBus().publish(DataTopic.MPPW_KAFKA_START.getValue(),
                    kafkaRequestContext.getRestLoadRequest().getRequestId());
        } else {
            requestMap.remove(kafkaRequestContext.getRestLoadRequest().getRequestId());
        }
    }

    private void handleMppwKafka(Message<String> requestMessage) {
        String requestId = requestMessage.body();
        final MppwKafkaRequestContext requestContext = requestMap.get(requestId);
        if (requestContext != null) {
            mppwStartHandler.handle(requestContext, ar -> {
                if (ar.succeeded()) {
                    vertx.eventBus().publish(DataTopic.MPPW_KAFKA_START.getValue(), Json.encode(requestContext));
                } else {
                    log.error("Error executing mppw request: {}", requestContext);
                    requestMap.remove(requestContext.getRestLoadRequest().getRequestId());
                }
            });
        }
    }

}
