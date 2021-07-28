package io.arenadata.dtm.query.execution.plugin.adp.check.service;

import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpCheckTableService")
public class AdpCheckTableService implements CheckTableService {
    @Override
    public Future<Void> check(CheckTableRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
