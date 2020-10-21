package ru.ibs.dtm.query.execution.core.factory;

import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;

public interface RollbackRequestContextFactory {

    RollbackRequestContext create(EdmlRequestContext context);
}
