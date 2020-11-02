package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("coreDmlService")
public class DmlServiceImpl implements DmlService<QueryResult> {
    private final Map<SqlKind, DmlExecutor<QueryResult>> executorMap;

    public DmlServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    public void execute(DmlRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
        executorMap.get(context.getQuery().getKind()).execute(context, handler);
    }

    @Override
    public void addExecutor(DmlExecutor<QueryResult> executor) {
        executorMap.put(executor.getSqlKind(), executor);
    }
}
