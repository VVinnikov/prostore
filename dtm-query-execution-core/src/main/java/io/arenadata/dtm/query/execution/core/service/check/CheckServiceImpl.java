package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.SystemMetadata;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.stereotype.Service;

import java.util.*;

@Service("coreCheckService")
public class CheckServiceImpl implements CheckService {
    private static final String CHECK_RESULT_COLUMN_NAME = "check_result";
    private final Map<CheckType, CheckExecutor> executorMap;

    public CheckServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public void execute(CheckContext context, Handler<AsyncResult<QueryResult>> handler) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        if (datamart == null || datamart.isEmpty()) {
            handler.handle(Future.failedFuture(
                    new IllegalArgumentException("Datamart must be specified for all tables and views")));
        } else {
            executorMap.get(context.getCheckType()).execute(context)
                    .onSuccess(result -> handler.handle(Future.succeededFuture(
                            createQueryResult(context.getRequest().getQueryRequest().getRequestId(), result))))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        }

    }

    @Override
    public void addExecutor(CheckExecutor executor) {
        executorMap.put(executor.getType(), executor);
    }

    private QueryResult createQueryResult(UUID requestId, String result) {
        QueryResult queryResult = new QueryResult();
      //  queryResult.setRequestId(requestId);
        queryResult.setMetadata(Collections.singletonList(ColumnMetadata.builder()
                .name(CHECK_RESULT_COLUMN_NAME)
            //    .systemMetadata(SystemMetadata.SCHEMA)
                .type(ColumnType.VARCHAR)
                .build()));
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(CHECK_RESULT_COLUMN_NAME, result);
        queryResult.setResult(Collections.singletonList(resultMap));
        return queryResult;
    }
}
