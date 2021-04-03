package io.arenadata.dtm.query.execution.core.delta.service;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.delta.dto.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.base.service.DatamartExecutionService;

import static io.arenadata.dtm.common.model.SqlProcessingType.DELTA;

public interface DeltaService<T> extends DatamartExecutionService<DeltaRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return DELTA;
    }
}
