package ru.ibs.dtm.query.execution.plugin.adg.service.impl.status;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.api.request.StatusRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.StatusService;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

@Service("adgStatusService")
public class AdgStatusService implements StatusService<StatusQueryResult> {
    private final KafkaConsumerMonitor kafkaConsumerMonitor;
    private final MppwProperties mppwProperties;

    public AdgStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor, MppwProperties mppwProperties) {
        this.kafkaConsumerMonitor = kafkaConsumerMonitor;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public void execute(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> handler) {
        if (context == null || context.getRequest() == null) {
            handler.handle(Future.failedFuture("StatusRequestContext should not be null"));
            return;
        }

        StatusRequest request = context.getRequest();
        String consumerGroup = mppwProperties.getConsumerGroup();

        kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroup, request.getTopic())
            .onSuccess(p -> {
                StatusQueryResult result = new StatusQueryResult();
                result.setPartitionInfo(p);
                handler.handle(Future.succeededFuture(result));
            })
            .onFailure(f -> handler.handle(Future.failedFuture(f)));
    }
}
