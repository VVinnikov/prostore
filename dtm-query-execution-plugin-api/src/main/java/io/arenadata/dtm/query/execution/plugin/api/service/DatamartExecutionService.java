package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;

public interface DatamartExecutionService<Context extends RequestContext<?>, Result> {

    void execute(Context context, AsyncHandler<Result> handler);

    SqlProcessingType getSqlProcessingType();
}
