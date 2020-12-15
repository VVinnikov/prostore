package io.arenadata.dtm.query.execution.core.verticle.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.eventbus.DataTopic;
import io.arenadata.dtm.query.execution.core.configuration.properties.VertxPoolProperties;
import io.arenadata.dtm.query.execution.core.verticle.TaskVerticle;
import io.arenadata.dtm.query.execution.core.verticle.TaskVerticleExecutor;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskVerticleExecutorImpl extends AbstractVerticle implements TaskVerticleExecutor {
    private final Map<String, Handler<Promise>> taskMap = new ConcurrentHashMap<>();
    private final Map<String, AsyncResult<?>> resultMap = new ConcurrentHashMap<>();
    private final VertxPoolProperties vertxPoolProperties;

    @Override
    public void start() throws Exception {
        val options = new DeploymentOptions()
            .setWorkerPoolSize(vertxPoolProperties.getTaskPool())
            .setWorker(true);
        for (int i = 0; i < vertxPoolProperties.getTaskPool(); i++) {
            vertx.deployVerticle(new TaskVerticle(taskMap, resultMap), options);
        }
    }

    @Override
    public <T> void execute(Handler<Promise<T>> codeHandler, AsyncHandler<T> resultHandler) {
        String taskId = UUID.randomUUID().toString();
        taskMap.put(taskId, (Handler) codeHandler);
        vertx.eventBus().request(
            DataTopic.START_WORKER_TASK.getValue(),
            taskId,
            new DeliveryOptions().setSendTimeout(vertxPoolProperties.getTaskTimeout()),
            ar -> {
                if (ar.succeeded()) {
                    resultHandler.handle((AsyncResult<T>) resultMap.remove(ar.result().body().toString()));
                } else {
                    taskMap.remove(taskId);
                    resultHandler.handleError(ar.cause());
                }
            });
    }
}
