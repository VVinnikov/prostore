package io.arenadata.dtm.query.execution.plugin.adg.rollback.service;

import io.arenadata.dtm.query.execution.plugin.adg.rollback.factory.ReverseHistoryTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
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
    public Future<Void> execute(RollbackRequest request) {
        return cartridgeClient.reverseHistoryTransfer(requestFactory.create(request));
    }
}
