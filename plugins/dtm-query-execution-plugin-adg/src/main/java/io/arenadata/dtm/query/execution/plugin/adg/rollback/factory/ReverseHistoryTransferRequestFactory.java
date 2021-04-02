package io.arenadata.dtm.query.execution.plugin.adg.rollback.factory;

import io.arenadata.dtm.query.execution.plugin.adg.rollback.dto.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;

public interface ReverseHistoryTransferRequestFactory {
    ReverseHistoryTransferRequest create(RollbackRequest request);
}
