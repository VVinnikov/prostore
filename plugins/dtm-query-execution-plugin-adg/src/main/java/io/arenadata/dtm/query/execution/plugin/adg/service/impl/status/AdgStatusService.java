package io.arenadata.dtm.query.execution.plugin.adg.service.impl.status;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.StatusRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adgStatusService")
public class AdgStatusService implements StatusService<StatusQueryResult> {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;
    private final MppwProperties mppwProperties;

    @Autowired
    public AdgStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor,
                            MppwProperties mppwProperties) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<StatusQueryResult> execute(StatusRequestContext context) {
        return Future.future(promise -> {
            if (context == null || context.getRequest() == null) {
                promise.fail(new DataSourceException("StatusRequestContext should not be null"));
                return;
            }

            StatusRequest request = context.getRequest();
            String consumerGroup = mppwProperties.getConsumerGroup();

            kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroup, request.getTopic())
                    .onSuccess(kafkaInfoResult -> {
                        StatusQueryResult result = new StatusQueryResult();
                        result.setPartitionInfo(kafkaInfoResult);
                        promise.complete(result);
                    })
                    .onFailure(promise::fail);
        });
    }
}
