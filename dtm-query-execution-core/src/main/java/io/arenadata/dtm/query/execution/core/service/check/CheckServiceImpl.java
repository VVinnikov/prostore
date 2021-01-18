package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.dto.check.CheckContext;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;
import org.tarantool.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service("coreCheckService")
public class CheckServiceImpl implements CheckService {
    private static final String CHECK_RESULT_COLUMN_NAME = "check_result";
    private final Map<CheckType, CheckExecutor> executorMap;

    public CheckServiceImpl() {
        this.executorMap = new HashMap<>();
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        String datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
        if (StringUtils.isEmpty(datamart)) {
            return Future.failedFuture(
                    new DtmException("Datamart must be specified for all tables and views"));
        } else {
            return executorMap.get(context.getCheckType())
                    .execute(context)
                    .map(result -> createQueryResult(context.getRequest().getQueryRequest().getRequestId(),
                            result));
        }
    }

    private QueryResult createQueryResult(UUID requestId, String result) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put(CHECK_RESULT_COLUMN_NAME, result);
        return QueryResult.builder()
                .requestId(requestId)
                .metadata(Collections.singletonList(ColumnMetadata.builder()
                        .name(CHECK_RESULT_COLUMN_NAME)
                        .type(ColumnType.VARCHAR)
                        .build()))
                .result(Collections.singletonList(resultMap))
                .build();
    }

    @Override
    public void addExecutor(CheckExecutor executor) {
        executorMap.put(executor.getType(), executor);
    }

}
