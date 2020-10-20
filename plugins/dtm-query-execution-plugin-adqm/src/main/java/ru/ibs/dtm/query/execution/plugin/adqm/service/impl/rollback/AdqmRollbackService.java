package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.rollback;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.sql.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.AdqmRollbackRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
import ru.ibs.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.RollbackService;

import java.util.Collections;

@Slf4j
@Service("adqmRollbackService")
public class AdqmRollbackService implements RollbackService<Void> {
    private final RollbackRequestFactory<AdqmRollbackRequest> rollbackRequestFactory;
    private final AdqmQueryExecutor adqmQueryExecutor;

    @Autowired
    public AdqmRollbackService(RollbackRequestFactory<AdqmRollbackRequest> rollbackRequestFactory,
                               AdqmQueryExecutor adqmQueryExecutor) {
        this.rollbackRequestFactory = rollbackRequestFactory;
        this.adqmQueryExecutor = adqmQueryExecutor;
    }

    @Override
    public void execute(RollbackRequestContext context, Handler<AsyncResult<Void>> handler) {
        try {
            val rollbackRequest = rollbackRequestFactory.create(context.getRequest());
            Promise<Void> promise = Promise.promise();
            Future<Void> executingFuture = promise.future();
            for (PreparedStatementRequest statement : rollbackRequest.getStatements()) {
                executingFuture.compose(v -> executeSql(statement.getSql()));
            }
            executingFuture.onSuccess(success -> handler.handle(Future.succeededFuture()))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Rollback error while executing context: [{}]: {}", context, e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Void> executeSql(String sql) {
        return Future.future(p -> adqmQueryExecutor.execute(sql, Collections.emptyList(), ar -> {
            if (ar.succeeded()) {
                p.complete();
            } else {
                log.error("Rollback error while executing sql: [{}]: {}", sql, ar.cause());
                p.fail(ar.cause());
            }
        }));
    }
}
