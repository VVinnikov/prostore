package io.arenadata.dtm.query.execution.plugin.api.service.mppw;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.vertx.core.Future;

public interface MppwService {
    Future<QueryResult> execute(MppwRequest request);
}
