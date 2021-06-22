package io.arenadata.dtm.query.execution.plugin.adb.rollback.service;

import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adbRollbackService")
public class AdbRollbackService implements RollbackService<Void> {

    private final RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory;
    private final DatabaseExecutor adbQueryExecutor;

    @Autowired
    public AdbRollbackService(RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory,
                              @Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor) {
        this.rollbackRequestFactory = rollbackRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<Void> execute(RollbackRequest request) {
        return Future.future(promise -> {
            val rollbackRequest = rollbackRequestFactory.create(request);
            adbQueryExecutor.executeUpdate(rollbackRequest.getTruncate().getSql())
                    .compose(v -> adbQueryExecutor.executeUpdate(rollbackRequest.getDeleteFromActual().getSql()))
                    .compose(v -> adbQueryExecutor.executeInTransaction(rollbackRequest.getEraseOps()))
                    .onSuccess(success -> promise.complete())
                    .onFailure(promise::fail);
        });
    }
}
