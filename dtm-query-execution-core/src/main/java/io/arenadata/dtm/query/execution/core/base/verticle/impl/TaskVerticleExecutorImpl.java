package io.arenadata.dtm.query.execution.core.base.verticle.impl;

import io.arenadata.dtm.common.eventbus.DataTopic;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.VertxPoolProperties;
import io.arenadata.dtm.query.execution.core.base.verticle.TaskVerticle;
import io.arenadata.dtm.query.execution.core.base.verticle.TaskVerticleExecutor;
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
                .setWorkerPoolName("task-verticle-pool")
                .setWorker(true);
        for (int i = 0; i < vertxPoolProperties.getTaskPool(); i++) {
            vertx.deployVerticle(new TaskVerticle(taskMap, resultMap), options);
        }
    }

    @Override
    public <T> Future<T> execute(Handler<Promise<T>> codeHandler) {
        return Future.future(promise -> {
            String taskId = UUID.randomUUID().toString();
            taskMap.put(taskId, (Handler) codeHandler);
            vertx.eventBus().request(
                    DataTopic.START_WORKER_TASK.getValue(),
                    taskId,
                    new DeliveryOptions().setSendTimeout(vertxPoolProperties.getTaskTimeout()),
                    ar -> {
                        taskMap.remove(taskId);
                        if (ar.succeeded()) {
                            promise.complete((T) resultMap.remove(ar.result().body().toString()).result());
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }
}
