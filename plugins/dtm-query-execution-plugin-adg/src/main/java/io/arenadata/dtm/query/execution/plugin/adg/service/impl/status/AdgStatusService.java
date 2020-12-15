package io.arenadata.dtm.query.execution.plugin.adg.service.impl.status;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.StatusRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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
    public void execute(StatusRequestContext context, AsyncHandler<StatusQueryResult> handler) {
        if (context == null || context.getRequest() == null) {
            handler.handleError(new DataSourceException("StatusRequestContext should not be null"));
            return;
        }

        StatusRequest request = context.getRequest();
        String consumerGroup = mppwProperties.getConsumerGroup();

        kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroup, request.getTopic())
            .onSuccess(p -> {
                StatusQueryResult result = new StatusQueryResult();
                result.setPartitionInfo(p);
                handler.handleSuccess(result);
            })
            .onFailure(handler::handleError);
    }
}
