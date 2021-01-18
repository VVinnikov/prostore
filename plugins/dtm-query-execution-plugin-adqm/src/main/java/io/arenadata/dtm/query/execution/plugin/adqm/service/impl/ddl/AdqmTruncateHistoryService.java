package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmTruncateHistoryQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("adqmTruncateHistoryService")
public class AdqmTruncateHistoryService implements TruncateHistoryService {

    private final DatabaseExecutor adqmQueryExecutor;
    private final AdqmTruncateHistoryQueriesFactory queriesFactory;

    @Autowired
    public AdqmTruncateHistoryService(DatabaseExecutor adqmQueryExecutor,
                                      AdqmTruncateHistoryQueriesFactory queriesFactory) {
        this.adqmQueryExecutor = adqmQueryExecutor;
        this.queriesFactory = queriesFactory;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest params) {
        return adqmQueryExecutor.execute(queriesFactory.insertIntoActualQuery(params))
                .compose(result -> adqmQueryExecutor.execute(queriesFactory.flushQuery(params)))
                .compose(result -> adqmQueryExecutor.execute(queriesFactory.optimizeQuery(params)))
                .compose(result -> Future.succeededFuture());
    }
}
