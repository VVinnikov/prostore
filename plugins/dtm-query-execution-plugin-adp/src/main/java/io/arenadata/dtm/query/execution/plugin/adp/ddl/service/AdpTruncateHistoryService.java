package io.arenadata.dtm.query.execution.plugin.adp.ddl.service;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.ddl.factory.TruncateHistoryFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service("adpTruncateHistoryService")
@AllArgsConstructor
public class AdpTruncateHistoryService implements TruncateHistoryService {

    private final DatabaseExecutor adpQueryExecutor;
    private final TruncateHistoryFactory queriesFactory;

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return request.getSysCn().isPresent() ? executeWithSysCn(request) : execute(request);
    }

    private Future<Void> execute(TruncateHistoryRequest request) {
        val query = queriesFactory.create(request);
        val preparedQuery = PreparedStatementRequest.onlySql(query);
        return adpQueryExecutor.executeInTransaction(Arrays.asList(preparedQuery));
    }

    private Future<Void> executeWithSysCn(TruncateHistoryRequest request) {
        return adpQueryExecutor.execute(queriesFactory.createWithSysCn(request))
                .compose(result -> Future.succeededFuture());
    }

}
