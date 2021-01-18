package io.arenadata.dtm.query.execution.plugin.api.service.ddl;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.vertx.core.Future;

public interface TruncateHistoryService {
    Future<Void> truncateHistory(TruncateHistoryRequest params);
}
