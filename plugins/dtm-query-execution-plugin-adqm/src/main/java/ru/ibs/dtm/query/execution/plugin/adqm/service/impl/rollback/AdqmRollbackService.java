package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.rollback;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.RollbackService;

@Service("adqmRollbackService")
public class AdqmRollbackService implements RollbackService<Void> {

    @Override
    public void execute(RollbackRequestContext context, Handler<AsyncResult<Void>> handler) {
        //FIXME
    }
}
