package ru.ibs.dtm.query.execution.plugin.adb.service.impl.status;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.query.execution.plugin.api.service.KafkaStatusService;
import ru.ibs.dtm.query.execution.plugin.api.status.KafkaStatusRequestContext;

@Service("adbKafkaStatusService")
public class AdbKafkaStatusService implements KafkaStatusService<StatusQueryResult> {

    @Override
    public void execute(KafkaStatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> handler) {
        //TODO реализовать
    }
}
