package io.arenadata.dtm.query.execution.plugin.adb.service.impl.status;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adbStatusService")
public class AdbStatusService implements StatusService {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;
    private final MppwProperties mppwProperties;

    @Autowired
    public AdbStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor,
                            MppwProperties mppwProperties) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<StatusQueryResult> execute(String topic) {
        return Future.future(promise -> {
            String consumerGroup = mppwProperties.getConsumerGroup();
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
