package io.arenadata.dtm.query.execution.core.delta.factory;

import io.arenadata.dtm.query.execution.core.delta.dto.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.delta.dto.query.DeltaQuery;

public interface DeltaQueryFactory {
    DeltaQuery create(DeltaRequestContext context);
}
