package io.arenadata.dtm.query.execution.plugin.adb.service.impl.rollback;

import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;

@Slf4j
@Service("adbRollbackService")
public class AdbRollbackService implements RollbackService<Void> {

    private final RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory;
    private final AdbQueryExecutor adbQueryExecutor;

    @Autowired
    public AdbRollbackService(RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory,
                              AdbQueryExecutor adbQueryExecutor) {
        this.rollbackRequestFactory = rollbackRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<Void> execute(RollbackRequestContext context) {
        return Future.future(promise -> {
            val rollbackRequest = rollbackRequestFactory.create(context.getRequest());
            adbQueryExecutor.execute(rollbackRequest.getTruncate().getSql(), Collections.emptyList())
                    .compose(v -> adbQueryExecutor.execute(rollbackRequest.getDeleteFromActual().getSql(),
                            Collections.emptyList()))
                    .compose(v -> adbQueryExecutor.executeInTransaction(
                            Arrays.asList(rollbackRequest.getInsert(), rollbackRequest.getDeleteFromHistory())
                    ))
                    .onSuccess(success -> promise.complete())
                    .onFailure(promise::fail);
        });
    }
}
