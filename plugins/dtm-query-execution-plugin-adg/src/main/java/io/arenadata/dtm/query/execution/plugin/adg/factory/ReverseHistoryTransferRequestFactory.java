package io.arenadata.dtm.query.execution.plugin.adg.factory;

import io.arenadata.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;

public interface ReverseHistoryTransferRequestFactory {
    ReverseHistoryTransferRequest create(RollbackRequestContext context);
}
