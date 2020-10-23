package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("adbDdlService")
public class AdbDdlService implements DdlService<Void> {

    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new HashMap<>();

    @Override
    public void execute(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
        SqlNode query = context.getQuery();
        if (query == null) {
            handler.handle(Future.failedFuture("Ddl query is null!"));
            return;
        }
        if (ddlExecutors.containsKey(query.getKind())) {
            ddlExecutors.get(query.getKind()).execute(context, query.getKind().lowerName, handler);
        } else {
            handler.handle(Future.failedFuture(String.format("Unknown DDL: %s", query)));
        }
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
