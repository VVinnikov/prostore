package io.arenadata.dtm.query.execution.plugin.adg.service.impl.rollback;

import io.arenadata.dtm.common.exception.CrashException;
import io.arenadata.dtm.query.execution.plugin.adg.factory.ReverseHistoryTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service("adgRollbackService")
public class AdgRollbackService implements RollbackService<Void> {
    private static final String ERR_MSG = "Can't rollback delta";
    private final ReverseHistoryTransferRequestFactory requestFactory;
    private final AdgCartridgeClient cartridgeClient;

    @Override
    public void execute(RollbackRequestContext context, Handler<AsyncResult<Void>> handler) {
        try {
            cartridgeClient.reverseHistoryTransfer(requestFactory.create(context), ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture());
                } else {
                    log.error(ERR_MSG, ar.cause());
                    handler.handle(Future.failedFuture(new CrashException(ERR_MSG, ar.cause())));
                }
            });
        } catch (Exception ex) {
            log.error(ERR_MSG, ex);
            handler.handle(Future.failedFuture(new CrashException(ERR_MSG, ex)));
        }
    }
}
