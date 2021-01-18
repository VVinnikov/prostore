package io.arenadata.dtm.query.execution.plugin.api.service.dml;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.DatamartExecutionService;

public interface DmlService<T> extends DatamartExecutionService<DmlRequestContext, T> {

  default SqlProcessingType getSqlProcessingType() {
    return SqlProcessingType.DML;
  }

  void addExecutor(DmlExecutor<T> executor);
}
