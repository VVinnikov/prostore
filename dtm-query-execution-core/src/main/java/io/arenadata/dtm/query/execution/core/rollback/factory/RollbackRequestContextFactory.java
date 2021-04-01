package io.arenadata.dtm.query.execution.core.rollback.factory;

import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequestContext;

public interface RollbackRequestContextFactory {

    RollbackRequestContext create(EdmlRequestContext context);
}
