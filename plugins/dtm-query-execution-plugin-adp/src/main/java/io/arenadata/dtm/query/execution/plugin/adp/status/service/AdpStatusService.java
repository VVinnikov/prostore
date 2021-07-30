package io.arenadata.dtm.query.execution.plugin.adp.status.service;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.query.execution.plugin.adp.base.properties.AdpMppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adpStatusService")
public class AdpStatusService implements StatusService {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;
    private final AdpMppwProperties mppwProperties;

    @Autowired
    public AdpStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor,
                            AdpMppwProperties mppwProperties) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<StatusQueryResult> execute(String topic) {
        return Future.future(promise -> {
            String consumerGroup = mppwProperties.getKafkaConsumerGroup();
            kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroup, topic)
                    .onSuccess(kafkaInfoResult -> {
                        StatusQueryResult result = new StatusQueryResult();
                        result.setPartitionInfo(kafkaInfoResult);
                        promise.complete(result);
                    })
                    .onFailure(promise::fail);
        });
    }
}
