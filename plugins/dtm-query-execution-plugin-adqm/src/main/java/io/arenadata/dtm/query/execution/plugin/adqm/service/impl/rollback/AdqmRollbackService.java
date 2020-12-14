package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.rollback;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
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
            Future<Void> executingFuture = Future.succeededFuture();
            for (PreparedStatementRequest statement : rollbackRequest.getStatements()) {
               executingFuture = executingFuture.compose(v -> executeSql(statement.getSql()));
            }
            executingFuture.onSuccess(success -> handler.handle(Future.succeededFuture()))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Rollback error while executing context: [{}]: {}", context, e);
            handler.handle(Future.failedFuture(
                    new RollbackDatasourceException(String.format("Rollback error while executing request %s",
                            context.getRequest()), e)));
        }
    }

    private Future<Void> executeSql(String sql) {
        return Future.future(p -> adqmQueryExecutor.executeUpdate(sql, ar -> {
            if (ar.succeeded()) {
                p.complete();
            } else {
                p.fail(ar.cause());
            }
        }));
    }
}
