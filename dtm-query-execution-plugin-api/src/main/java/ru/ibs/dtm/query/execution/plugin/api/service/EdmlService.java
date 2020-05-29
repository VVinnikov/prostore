package ru.ibs.dtm.query.execution.plugin.api.service;

import io.vertx.core.AsyncResult;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;


public interface EdmlService<T> extends DatamartExecutionService<EdmlRequestContext, AsyncResult<T>> {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.EDML;
  }

}
