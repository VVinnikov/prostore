package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.vertx.core.AsyncResult;


public interface EdmlService<T> extends DatamartExecutionService<EdmlRequestContext, AsyncResult<T>> {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.EDML;
  }

}
