package io.arenadata.dtm.query.execution.plugin.adp.rollback.service;

import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.rollback.factory.AdpRollbackSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import static io.arenadata.dtm.query.execution.plugin.adp.rollback.factory.AdpRollbackSqlFactory.getRollbackSql;

@Slf4j
@Service("adpRollbackService")
public class AdpRollbackService implements RollbackService<Void> {

    private final DatabaseExecutor databaseExecutor;

    public AdpRollbackService(DatabaseExecutor databaseExecutor) {
        this.databaseExecutor = databaseExecutor;
    }

    @Override
    public Future<Void> execute(RollbackRequest request) {
        return Future.future(promise -> {
            log.info("[ADP] Start rollback");

            databaseExecutor.executeUpdate(getRollbackSql(request.getDatamartMnemonic(), request.getEntity(), request.getSysCn()))
                    .onSuccess(v -> {
                        log.debug("[ADP] Rollback finished successfully");
                        promise.complete();
                    })
                    .onFailure(t -> {
                        log.error("[ADP] Rollback failed", t);
                        promise.fail(t);
                    });
        });
    }
}
