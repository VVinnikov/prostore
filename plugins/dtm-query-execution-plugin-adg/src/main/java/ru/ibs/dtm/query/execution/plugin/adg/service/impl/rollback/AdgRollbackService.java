package ru.ibs.dtm.query.execution.plugin.adg.service.impl.rollback;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.RollbackService;

@Service("adgRollbackService")
public class AdgRollbackService implements RollbackService<Void> {

    @Override
    public void execute(RollbackRequestContext context, Handler<AsyncResult<Void>> handler) {
        //FIXME
    }
}
