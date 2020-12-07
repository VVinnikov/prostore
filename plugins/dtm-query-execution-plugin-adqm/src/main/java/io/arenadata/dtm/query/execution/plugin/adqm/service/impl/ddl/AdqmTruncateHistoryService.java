package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adqmTruncateHistoryService")
public class AdqmTruncateHistoryService implements TruncateHistoryService {
    @Override
    public Future<Void> truncateHistory(TruncateHistoryParams params) {
        return Future.failedFuture("Implementation not done yet");
    }
}
