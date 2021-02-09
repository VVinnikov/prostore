package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.service.DatamartExecutionService;

public interface DmlService<T> extends DatamartExecutionService<DmlRequestContext, T> {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.DML;
  }

  void addExecutor(DmlExecutor<T> executor);
}
