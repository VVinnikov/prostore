package io.arenadata.dtm.query.execution.plugin.adg.factory;

import io.arenadata.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;

public interface ReverseHistoryTransferRequestFactory {
    ReverseHistoryTransferRequest create(RollbackRequest request);
}
