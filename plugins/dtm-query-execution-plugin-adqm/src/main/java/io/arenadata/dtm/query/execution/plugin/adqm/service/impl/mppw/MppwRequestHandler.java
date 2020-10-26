package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.vertx.core.Future;

public interface MppwRequestHandler {
    Future<QueryResult> execute(MppwRequest request);
}
