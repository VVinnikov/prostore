package io.arenadata.dtm.query.execution.plugin.adg.synchronize.service;

import io.arenadata.dtm.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.SynchronizeService;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adgSynchronizeService")
public class AdgSynchronizeService implements SynchronizeService {
    @Override
    public Future<Long> execute(SynchronizeRequest request) {
        return Future.failedFuture(new SynchronizeDatasourceException("Synchronize[ADG] is not implemented"));
    }
}
