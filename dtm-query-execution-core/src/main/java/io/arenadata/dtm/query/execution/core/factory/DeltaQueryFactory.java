package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.core.dto.delta.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaQuery;

public interface DeltaQueryFactory {
    DeltaQuery create(DeltaRequestContext context);
}
