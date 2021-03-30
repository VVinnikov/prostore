package io.arenadata.dtm.query.execution.core.service.ddl;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.service.DatamartExecutionService;

public interface DdlService<T> extends DatamartExecutionService<DdlRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.DDL;
    }

    void addExecutor(DdlExecutor<T> executor);
}
