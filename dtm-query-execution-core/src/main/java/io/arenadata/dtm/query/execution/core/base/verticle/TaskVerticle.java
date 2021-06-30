package io.arenadata.dtm.query.execution.core.base.verticle;

import io.arenadata.dtm.common.eventbus.DataTopic;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;

@Slf4j
public class TaskVerticle extends AbstractVerticle {
    private final Map<String, Handler<Promise>> taskMap;
    private final Map<String, AsyncResult<?>> resultMap;

    public TaskVerticle(Map<String, Handler<Promise>> taskMap, Map<String, AsyncResult<?>> resultMap) {
        this.taskMap = taskMap;
        this.resultMap = resultMap;
    }

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(DataTopic.START_WORKER_TASK.getValue(), this::handle);
    }

    private void handle(Message<String> tMessage) {
        String requestId = tMessage.body();
        val task = taskMap.remove(requestId);
        Future.future(task::handle)
                .onComplete(ar -> {
                    resultMap.put(requestId, ar);
                    tMessage.reply(requestId);
                });
    }
}
