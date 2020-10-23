package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.vertx.core.AsyncResult;


public interface DmlService<T> extends DatamartExecutionService<DmlRequestContext, AsyncResult<T>> {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.DML;
  }
}
