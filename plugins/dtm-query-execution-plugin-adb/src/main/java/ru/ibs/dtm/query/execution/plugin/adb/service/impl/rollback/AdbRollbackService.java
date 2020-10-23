package ru.ibs.dtm.query.execution.plugin.adb.service.impl.rollback;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.sql.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.adb.dto.AdbRollbackRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import ru.ibs.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.RollbackService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    public void execute(RollbackRequestContext context, Handler<AsyncResult<Void>> handler) {
        val rollbackRequest = rollbackRequestFactory.create(context.getRequest());
        CompositeFuture.join(Arrays.asList(
            executeSql(rollbackRequest.getTruncate().getSql()),
            executeSql(rollbackRequest.getDeleteFromActual().getSql()),
            executeSqlInTran(Arrays.asList(rollbackRequest.getInsert(), rollbackRequest.getDeleteFromHistory()))))
            .onSuccess(success -> handler.handle(Future.succeededFuture()))
            .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
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
