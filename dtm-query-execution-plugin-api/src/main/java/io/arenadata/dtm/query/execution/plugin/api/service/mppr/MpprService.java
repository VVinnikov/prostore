package io.arenadata.dtm.query.execution.plugin.api.service.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.vertx.core.Future;

public interface MpprService {
    Future<QueryResult> execute(MpprRequest request);
}
