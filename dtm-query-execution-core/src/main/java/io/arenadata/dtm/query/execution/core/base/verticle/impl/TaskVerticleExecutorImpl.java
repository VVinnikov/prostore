package io.arenadata.dtm.query.execution.core.base.verticle.impl;

import io.arenadata.dtm.common.eventbus.DataTopic;
import io.arenadata.dtm.query.execution.core.base.configuration.properties.VertxPoolProperties;
import io.arenadata.dtm.query.execution.core.base.verticle.TaskVerticle;
import io.arenadata.dtm.query.execution.core.base.verticle.TaskVerticleExecutor;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
    public void start(Promise<Void> startPromise) throws Exception {
        DeploymentOptions options = new DeploymentOptions()
                .setInstances(vertxPoolProperties.getTaskPool());
        vertx.deployVerticle(() -> new TaskVerticle(taskMap, resultMap), options, ar -> {
            if (ar.succeeded()) {
                startPromise.complete();
            } else {
                startPromise.fail(ar.cause());
            }
        });
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
                            promise.handle((AsyncResult<T>) resultMap.remove(ar.result().body().toString()));
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }
}
