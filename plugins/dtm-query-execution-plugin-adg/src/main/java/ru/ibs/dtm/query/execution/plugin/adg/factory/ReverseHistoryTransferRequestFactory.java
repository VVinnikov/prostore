package ru.ibs.dtm.query.execution.plugin.adg.factory;

import ru.ibs.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;

public interface ReverseHistoryTransferRequestFactory {
    ReverseHistoryTransferRequest create(RollbackRequestContext context);
}
