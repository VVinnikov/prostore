package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;

public interface DatamartExecutionService<Context extends RequestContext<?>, Result> {
	void execute(Context context, Handler<Result> handler);
	SqlProcessingType getSqlProcessingType();
}
