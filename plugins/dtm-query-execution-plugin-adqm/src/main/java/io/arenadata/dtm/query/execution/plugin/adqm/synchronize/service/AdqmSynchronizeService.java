package io.arenadata.dtm.query.execution.plugin.adqm.synchronize.service;

import io.arenadata.dtm.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.SynchronizeService;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adqmSynchronizeService")
public class AdqmSynchronizeService implements SynchronizeService {
    @Override
    public Future<Long> execute(SynchronizeRequest request) {
        return Future.failedFuture(new SynchronizeDatasourceException("Synchronize[ADQM] is not implemented"));
    }
}
