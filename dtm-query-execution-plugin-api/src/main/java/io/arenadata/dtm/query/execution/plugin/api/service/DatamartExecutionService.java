package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.vertx.core.Handler;

public interface DatamartExecutionService<Context extends RequestContext<?>, Result> {
	void execute(Context context, Handler<Result> handler);
	SqlProcessingType getSqlProcessingType();
}
