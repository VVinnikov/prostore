package io.arenadata.dtm.query.execution.plugin.adp.status.service;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpStatusService")
public class AdpStatusService implements StatusService {
    @Override
    public Future<StatusQueryResult> execute(String topic) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
