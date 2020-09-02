package ru.ibs.dtm.query.execution.plugin.adb.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import ru.ibs.dtm.common.eventbus.DataTopic;

public abstract class MppwVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(DataTopic.MPPW_START.getValue(), this::handle);
        vertx.eventBus().consumer(DataTopic.MPPW_KAFKA_START.getValue(), this::handleMppwKafka);
    }

    protected abstract void handle(Message<String> requestMessage);

    protected abstract void handleMppwKafka(Message<String> requsetMessage);
}
