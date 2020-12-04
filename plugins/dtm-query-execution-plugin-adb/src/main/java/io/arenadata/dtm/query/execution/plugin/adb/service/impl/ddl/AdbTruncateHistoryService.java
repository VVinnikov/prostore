package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("adbTruncateHistoryService")
public class AdbTruncateHistoryService implements TruncateHistoryService {
    private static final String DELETE_RECORDS_PATTERN = "DELETE FROM %s.%s_%s%s";
    private static final String SYS_CN_CONDITION = "sys_to < %s";
    private final DatabaseExecutor adbQueryExecutor;

    @Autowired
    public AdbTruncateHistoryService(DatabaseExecutor adbQueryExecutor) {
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryParams params) {
        return params.getSysCn().isPresent() ? executeWithSysCn(params) : execute(params);
    }

    private Future<Void> execute(TruncateHistoryParams params) {
        String whereExpression = params.getConditions()
                .map(conditions -> String.format(" WHERE %s", conditions))
                .orElse("");
        return Future.future(promise -> CompositeFuture.join(
                adbQueryExecutor.execute(String.format(DELETE_RECORDS_PATTERN, params.getSchema(), params.getTable(),
                        AdbTables.ACTUAL_TABLE_POSTFIX, whereExpression)),
                adbQueryExecutor.execute(String.format(DELETE_RECORDS_PATTERN, params.getSchema(), params.getTable(),
                        AdbTables.HISTORY_TABLE_POSTFIX, whereExpression)))
                .onSuccess(result -> promise.complete())
                .onFailure(promise::fail));
    }

    private Future<Void> executeWithSysCn(TruncateHistoryParams params) {
        String query = String.format(DELETE_RECORDS_PATTERN, params.getSchema(), params.getTable(),
                AdbTables.HISTORY_TABLE_POSTFIX, String.format(" WHERE %s%s", params.getConditions()
                                .map(conditions -> String.format("%s AND ", conditions))
                                .orElse(""),
                        String.format(SYS_CN_CONDITION, params.getSysCn().get())));
        return adbQueryExecutor.execute(query)
                .compose(result -> Future.succeededFuture());
    }
}
