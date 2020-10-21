package ru.ibs.dtm.query.execution.plugin.api.factory;

import ru.ibs.dtm.query.execution.plugin.api.request.RollbackRequest;
import ru.ibs.dtm.query.execution.plugin.api.rollback.PluginRollbackRequest;

public interface RollbackRequestFactory<T extends PluginRollbackRequest> {
    T create(RollbackRequest rollbackRequest);
}
