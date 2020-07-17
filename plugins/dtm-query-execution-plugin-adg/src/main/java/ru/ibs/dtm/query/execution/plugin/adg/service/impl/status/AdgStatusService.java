package ru.ibs.dtm.query.execution.plugin.adg.service.impl.status;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import ru.ibs.dtm.query.execution.plugin.api.service.StatusService;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;

@Service("adgStatusService")
public class AdgStatusService implements StatusService<StatusQueryResult> {

    @Override
    public void execute(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> handler) {
        //TODO реализовать
        StatusQueryResult result = new StatusQueryResult();
        KafkaPartitionInfo partitionInfo = new KafkaPartitionInfo();
        partitionInfo.setEnd(3L);
        partitionInfo.setOffset(3L);
        partitionInfo.setLastCommitTime(LocalDateTime.now().plus(0, ChronoField.MILLI_OF_DAY.getBaseUnit()));
        result.setPartitionInfo(partitionInfo);
        handler.handle(Future.succeededFuture(result));
    }
}
