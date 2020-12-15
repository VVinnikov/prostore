package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface QueryDispatcher {

	void dispatch(RequestContext<? extends DatamartRequest> context, AsyncHandler<QueryResult> handler);

}
