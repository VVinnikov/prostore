package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.rollback;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
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
    public Future<Void> execute(RollbackRequest request) {
        return Future.future(promise -> {
            val rollbackRequest = rollbackRequestFactory.create(request);
            Future<Void> executingFuture = Future.succeededFuture();
            for (PreparedStatementRequest statement : rollbackRequest.getStatements()) {
                executingFuture = executingFuture.compose(v -> adqmQueryExecutor.executeUpdate(statement.getSql()));
            }
            executingFuture.onSuccess(success -> promise.complete())
                    .onFailure(promise::fail);
        });
    }
}
