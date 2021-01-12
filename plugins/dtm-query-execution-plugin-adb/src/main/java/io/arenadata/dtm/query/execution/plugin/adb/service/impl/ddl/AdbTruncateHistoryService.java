package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.TruncateHistoryDeleteQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

@Service("adbTruncateHistoryService")
public class AdbTruncateHistoryService implements TruncateHistoryService {
    private final DatabaseExecutor adbQueryExecutor;
    private final TruncateHistoryDeleteQueriesFactory queriesFactory;

    @Autowired
    public AdbTruncateHistoryService(DatabaseExecutor adbQueryExecutor,
                                     TruncateHistoryDeleteQueriesFactory queriesFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.queriesFactory = queriesFactory;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryParams params) {
        return params.getSysCn().isPresent() ? executeWithSysCn(params) : execute(params);
    }

    private Future<Void> execute(TruncateHistoryParams params) {
        val queries = queriesFactory.create(params);
        return adbQueryExecutor.executeInTransaction(queries.stream()
                .map(PreparedStatementRequest::onlySql)
                .collect(Collectors.toList()));
    }

    private Future<Void> executeWithSysCn(TruncateHistoryParams params) {
        return adbQueryExecutor.execute(queriesFactory.createWithSysCn(params))
                .compose(result -> Future.succeededFuture());
    }
}
