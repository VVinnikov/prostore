package io.arenadata.dtm.query.execution.core.ddl.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;

public interface DdlService<T> extends DatamartExecutionService<DdlRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.DDL;
    }

    void addExecutor(DdlExecutor<T> executor);
}
