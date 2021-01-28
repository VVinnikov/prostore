package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.vertx.core.Future;

public interface DatamartExecutionService<Context extends RequestContext<?>, Result> {

    Future<Result> execute(Context context);

    SqlProcessingType getSqlProcessingType();
}
