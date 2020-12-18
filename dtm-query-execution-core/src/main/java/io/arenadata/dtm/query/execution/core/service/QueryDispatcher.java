package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.Future;

public interface QueryDispatcher {

    Future<QueryResult> dispatch(RequestContext<? extends DatamartRequest> context);
}
