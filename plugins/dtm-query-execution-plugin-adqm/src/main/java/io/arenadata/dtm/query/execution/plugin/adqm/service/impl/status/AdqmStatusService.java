package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.status;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.StatusReportDto;
import io.arenadata.dtm.query.execution.plugin.adqm.service.StatusReporter;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.vertx.core.Future;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("adqmStatusService")
@Slf4j
public class AdqmStatusService implements StatusService, StatusReporter {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;
    private final Map<String, String> topicsInUse = new HashMap<>();

    public AdqmStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
    }

    @Override
    public Future<StatusQueryResult> execute(String topic) {
        return Future.future(promise -> {
            if (topicsInUse.containsKey(topic)) {
                String consumerGroup = topicsInUse.get(topic);
                kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroup, topic)
                        .onSuccess(kafkaInfoResult -> {
                            StatusQueryResult result = new StatusQueryResult();
                            result.setPartitionInfo(kafkaInfoResult);
                            promise.complete(result);
                        })
                        .onFailure(promise::fail);
            } else {
                promise.fail("Topic isn't used");
            }
        });
    }

    @Override
    public void onStart(@NonNull final StatusReportDto payload) {
        String topic = payload.getTopic();
        String consumerGroup = payload.getConsumerGroup();
        topicsInUse.put(topic, consumerGroup);
    }

    @Override
    public void onFinish(@NonNull final StatusReportDto payload) {
        topicsInUse.remove(payload.getTopic());
    }

    @Override
    public void onError(@NonNull final StatusReportDto payload) {
        topicsInUse.remove(payload.getTopic());
    }
}
