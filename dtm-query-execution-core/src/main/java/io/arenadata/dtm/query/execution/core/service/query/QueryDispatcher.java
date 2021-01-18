package io.arenadata.dtm.query.execution.core.service.query;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import io.vertx.core.Future;

public interface QueryDispatcher {

    Future<QueryResult> dispatch(CoreRequestContext<? extends DatamartRequest> context);
}
