package io.arenadata.dtm.query.execution.core.dml.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;

public interface DmlService<T> extends DatamartExecutionService<DmlRequestContext, T> {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.DML;
  }

  void addExecutor(DmlExecutor<T> executor);
}
