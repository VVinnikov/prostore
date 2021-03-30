package io.arenadata.dtm.query.execution.plugin.api.factory;

import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.PluginRollbackRequest;

public interface RollbackRequestFactory<T extends PluginRollbackRequest> {
    T create(RollbackRequest rollbackRequest);
}
