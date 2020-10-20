package ru.ibs.dtm.query.execution.plugin.adb.service.impl.rollback;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adb.factory.RollbackRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.RollbackService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Service("adbRollbackService")
public class AdbRollbackService implements RollbackService<Void> {

    private final RollbackRequestFactory rollbackRequestFactory;
    private final AdbQueryExecutor adbQueryExecutor;

    @Autowired
    public AdbRollbackService(RollbackRequestFactory rollbackRequestFactory, AdbQueryExecutor adbQueryExecutor) {
        this.rollbackRequestFactory = rollbackRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public void execute(RollbackRequestContext context, Handler<AsyncResult<Void>> handler) {
        List<PreparedStatementRequest> rollbackSqlRequests = rollbackRequestFactory.create(context.getRequest());
        CompositeFuture.join(Arrays.asList(
                executeSql(rollbackSqlRequests.get(0).getSql()),
                executeSql(rollbackSqlRequests.get(1).getSql()),
                executeSqlInTran(rollbackSqlRequests.subList(2, 4))))
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
