package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;

public interface RollbackRequestContextFactory {

    RollbackRequestContext create(EdmlRequestContext context);
}
