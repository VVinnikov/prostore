package io.arenadata.dtm.query.execution.plugin.adp.synchronize.service;

import io.arenadata.dtm.query.execution.plugin.api.service.SynchronizeService;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpSynchronizeService")
public class AdpSynchronizeService implements SynchronizeService {
    @Override
    public Future<Long> execute(SynchronizeRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
