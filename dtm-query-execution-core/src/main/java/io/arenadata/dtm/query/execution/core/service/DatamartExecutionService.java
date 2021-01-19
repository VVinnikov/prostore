package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.dto.CoreRequestContext;
import io.vertx.core.Future;

public interface DatamartExecutionService<Context extends CoreRequestContext<?,?>, Result> {

    Future<Result> execute(Context context);

    SqlProcessingType getSqlProcessingType();
}
