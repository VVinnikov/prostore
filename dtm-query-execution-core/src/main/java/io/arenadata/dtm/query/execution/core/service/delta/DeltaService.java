package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.dto.delta.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.service.DatamartExecutionService;

import static io.arenadata.dtm.common.model.SqlProcessingType.DELTA;

public interface DeltaService<T> extends DatamartExecutionService<DeltaRequestContext, T> {

    default SqlProcessingType getSqlProcessingType() {
        return DELTA;
    }
}
