package io.arenadata.dtm.query.execution.plugin.adp.rollback.service;

import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpRollbackService")
public class AdpRollbackService implements RollbackService<Void> {
    @Override
    public Future<Void> execute(RollbackRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
