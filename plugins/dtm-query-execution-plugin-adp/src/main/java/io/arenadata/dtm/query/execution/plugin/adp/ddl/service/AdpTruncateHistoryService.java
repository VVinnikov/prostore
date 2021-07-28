package io.arenadata.dtm.query.execution.plugin.adp.ddl.service;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpTruncateHistoryService")
public class AdpTruncateHistoryService implements TruncateHistoryService {
    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
