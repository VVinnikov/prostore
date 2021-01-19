package io.arenadata.dtm.query.execution.plugin.adg.service.impl.rollback;

import io.arenadata.dtm.query.execution.plugin.adg.factory.ReverseHistoryTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service("adgRollbackService")
public class AdgRollbackService implements RollbackService<Void> {

    private final ReverseHistoryTransferRequestFactory requestFactory;
    private final AdgCartridgeClient cartridgeClient;

    @Override
    public Future<Void> execute(RollbackRequestContext request) {
        return cartridgeClient.reverseHistoryTransfer(requestFactory.create(request));
    }
}
