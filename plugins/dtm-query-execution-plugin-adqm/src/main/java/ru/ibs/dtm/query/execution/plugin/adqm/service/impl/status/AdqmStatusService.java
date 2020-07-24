package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.status;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.StatusReporter;
import ru.ibs.dtm.query.execution.plugin.api.request.StatusRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.StatusService;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

import java.util.HashMap;
import java.util.Map;

@Service("adqmStatusService")
@Slf4j
public class AdqmStatusService implements StatusService<StatusQueryResult>, StatusReporter {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;
    private final Map<String, String> topicsInUse = new HashMap<>();

    public AdqmStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
    }

    @Override
    public void execute(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> handler) {
        if (context == null || context.getRequest() == null) {
            handler.handle(Future.failedFuture("StatusRequestContext should not be null"));
            return;
        }

        StatusRequest request = context.getRequest();

        if (topicsInUse.containsKey(request.getTopic())) {
            String consumerGroup = topicsInUse.get(request.getTopic());
            StatusQueryResult result = new StatusQueryResult();
            result.setPartitionInfo(
                    kafkaConsumerMonitor.getAggregateGroupConsumerInfo(
                            consumerGroup, request.getTopic()));
            handler.handle(Future.succeededFuture(result));
        } else {
            handler.handle(Future.failedFuture("Cannot find info about " + request.getTopic()));
        }
    }

    @Override
    public void onStart(@NonNull final JsonObject payload) {
        String topic = payload.getString("topic");
        String consumerGroup = payload.getString("consumerGroup");
        topicsInUse.put(topic, consumerGroup);
    }

    @Override
    public void onFinish(@NonNull final JsonObject payload) {
        String topic = payload.getString("topic");
        topicsInUse.remove(topic);
    }

    @Override
    public void onError(@NonNull final JsonObject payload) {
        String topic = payload.getString("topic");
        topicsInUse.remove(topic);
    }
}
