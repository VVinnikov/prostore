package ru.ibs.dtm.query.execution.plugin.adg.service.impl.status;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.query.execution.plugin.api.service.StatusService;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

@Service("adgStatusService")
public class AdgStatusService implements StatusService<StatusQueryResult> {

    @Override
    public void execute(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> handler) {
        //TODO реализовать
    }
}
