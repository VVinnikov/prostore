package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import ru.ibs.dtm.query.execution.plugin.api.dml.DmlRequestContext;


public interface DmlService<T> extends DatamartExecutionService<DmlRequestContext, AsyncResult<T>> {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.DML;
  }
}
