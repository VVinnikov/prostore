package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.rollback.RollbackRequestContext;

public interface RollbackRequestContextFactory {

    RollbackRequestContext create(EdmlRequestContext context);
}
