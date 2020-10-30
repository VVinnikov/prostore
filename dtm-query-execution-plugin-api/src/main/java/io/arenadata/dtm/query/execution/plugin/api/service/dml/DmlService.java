package io.arenadata.dtm.query.execution.plugin.api.service.dml;

import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import io.vertx.core.AsyncResult;

public interface DmlService<T> extends DatamartExecutionService<DmlRequestContext, AsyncResult<T>> {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.DML;
  }

  void addExecutor(DmlExecutor<T> executor);
}
