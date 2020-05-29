package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

public interface QueryDispatcher {

	void dispatch(RequestContext<? extends DatamartRequest> context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

}
