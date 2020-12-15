package io.arenadata.dtm.query.execution.plugin.adb.service.impl.rollback;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.RollbackDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service("adbRollbackService")
public class AdbRollbackService implements RollbackService<Void> {

    private final RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory;
    private final AdbQueryExecutor adbQueryExecutor;

    @Autowired
    public AdbRollbackService(RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory, AdbQueryExecutor adbQueryExecutor) {
        this.rollbackRequestFactory = rollbackRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public void execute(RollbackRequestContext context, AsyncHandler<Void> handler) {
        try {
            val rollbackRequest = rollbackRequestFactory.create(context.getRequest());
            executeSql(rollbackRequest.getTruncate().getSql())
                .compose(v -> executeSql(rollbackRequest.getDeleteFromActual().getSql()))
                .compose(v -> executeSqlInTran(
                    Arrays.asList(rollbackRequest.getInsert(), rollbackRequest.getDeleteFromHistory())
                ))
                .onSuccess(success -> handler.handleSuccess())
                .onFailure(handler::handleError);
        } catch (Exception e) {
            handler.handleError(
                    new RollbackDatasourceException(String.format("Rollback error while executing request [%s]",
                            context.getRequest()), e));
        }
    }

    private Future<Void> executeSql(String sql) {
        return Future.future(p -> adbQueryExecutor.execute(sql, Collections.emptyList(), ar -> {
            if (ar.succeeded()) {
                p.complete();
            } else {
                p.fail(ar.cause());
            }
        }));
    }

    private Future<Void> executeSqlInTran(List<PreparedStatementRequest> rollbackSqlRequests) {
        return Future.future(p -> adbQueryExecutor.executeInTransaction(rollbackSqlRequests, ar -> {
            if (ar.succeeded()) {
                p.complete();
            } else {
                p.fail(ar.cause());
            }
        }));
    }
}
